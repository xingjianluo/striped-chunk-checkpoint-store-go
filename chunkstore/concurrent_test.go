package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

// 多个写入方并发提交同一对象、内容相同：全部幂等成功，结果一致。
func TestConcurrentCommitSameContent(t *testing.T) {
	s := testStore(t, 16, 4)
	content := []byte("the quick brown fox jumps over the lazy dog, again and again!")
	const writers = 8

	var wg sync.WaitGroup
	digests := make([]string, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m, err := s.PutObject("shared", bytes.NewReader(content))
			if err == nil {
				digests[i] = m.Digest
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("writer %d: %v", i, errs[i])
		}
		if digests[i] != digests[0] {
			t.Fatalf("writer %d digest %s != %s", i, digests[i], digests[0])
		}
	}
	fmt.Printf("判定依据：%d 个写入方提交相同内容，digest 全部一致=%s（幂等）\n", writers, digests[0])

	r, err := s.OpenObject("shared")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch")
	}
}

// 并发提交同一对象、内容不同：恰有一个获胜者，
// 其余收到 *ConflictError；任何时刻读到的都是完整对象。
func TestConcurrentCommitConflict(t *testing.T) {
	s := testStore(t, 8, 2)
	const writers = 6
	contents := make([][]byte, writers)
	for i := range contents {
		contents[i] = bytes.Repeat([]byte{byte('A' + i)}, 40)
	}

	var wg sync.WaitGroup
	results := make([]error, writers)
	digests := make([]string, writers)
	var readers sync.WaitGroup
	stop := make(chan struct{})
	// 读取方循环读取，断言永远读不到半提交状态。
	for k := 0; k < 2; k++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r, err := s.OpenObject("race")
				if errors.Is(err, ErrObjectNotFound) {
					continue
				}
				if err != nil {
					t.Errorf("reader saw broken object: %v", err)
					return
				}
				got, err := io.ReadAll(r)
				r.Close()
				if err != nil {
					t.Errorf("reader read error: %v", err)
					return
				}
				ok := false
				for _, c := range contents {
					if bytes.Equal(got, c) {
						ok = true
						break
					}
				}
				if !ok {
					t.Errorf("reader saw half-committed content: %q", got)
					return
				}
			}
		}()
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m, err := s.PutObject("race", bytes.NewReader(contents[i]))
			if m != nil {
				digests[i] = m.Digest
			}
			results[i] = err
		}(i)
	}
	wg.Wait()
	close(stop)
	readers.Wait()

	var wins, conflicts int
	for i, err := range results {
		if err == nil {
			wins++
		} else {
			var ce *ConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("writer %d unexpected error: %v", i, err)
			}
			conflicts++
			if ce.ExistingDigest != digests[i] {
				t.Fatalf("writer %d: conflict digest %s != returned %s",
					i, ce.ExistingDigest, digests[i])
			}
		}
	}
	if wins != 1 || conflicts != writers-1 {
		t.Fatalf("wins=%d conflicts=%d, want 1 and %d", wins, conflicts, writers-1)
	}
	fmt.Printf("判定依据：先提交者获胜 wins=%d，其余 conflicts=%d 均返回 ConflictError，获胜 digest=%s\n",
		wins, conflicts, digests[0])

	// 最终内容必须是获胜者的完整内容。
	r, err := s.OpenObject("race")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	found := false
	for _, c := range contents {
		if bytes.Equal(got, c) {
			found = true
		}
	}
	if !found {
		t.Fatal("final content is not any writer's complete content")
	}
}

// 同一上传会话重复提交：幂等，第二次返回相同清单。
func TestDuplicateCommitIdempotent(t *testing.T) {
	s := testStore(t, 16, 2)
	content := []byte("duplicate commit should be idempotent")
	u, err := s.BeginUpload("dup", int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.CopyFrom(bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	m1, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	// 已提交的会话不能再次写入或提交。
	if _, err := u.CopyFrom(bytes.NewReader(content)); !errors.Is(err, ErrCommitted) {
		t.Fatalf("want ErrCommitted, got %v", err)
	}
	if _, err := u.Commit(); !errors.Is(err, ErrCommitted) {
		t.Fatalf("want ErrCommitted, got %v", err)
	}

	// 不同会话提交相同内容：幂等成功且 digest 相同。
	m2, err := s.PutObject("dup", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if m2.Digest != m1.Digest {
		t.Fatalf("digest changed on idempotent re-commit: %s -> %s", m1.Digest, m2.Digest)
	}
	fmt.Printf("判定依据：重复提交 digest 不变=%s（幂等）\n", m1.Digest)
}
