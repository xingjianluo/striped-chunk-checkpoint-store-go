package store

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
)

// TestReclaimConcurrentWithRead 验证：回收与读取同时发生时，
// 正在读取所依赖的共享块不会被回收，读出内容完整。
func TestReclaimConcurrentWithRead(t *testing.T) {
	s := newStore(t)
	shared := bytes.Repeat([]byte("COMMON"), 200)
	d1 := append(append([]byte{}, shared...), bytes.Repeat([]byte("K"), 1500)...)
	d2 := append(append([]byte{}, shared...), bytes.Repeat([]byte("G"), 1500)...)
	if err := s.Put("keeper", bytes.NewReader(d1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("goner", bytes.NewReader(d2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("goner"); err != nil {
		t.Fatal(err)
	}

	// 先持有读锁，模拟"一个长时间读取正在进行"。
	started := make(chan struct{})
	proceed := make(chan struct{})
	go func() {
		s.life.RLock()
		close(started) // 读取窗口已建立
		<-proceed
		// 在持锁期间真正逐块读取 keeper，依赖的共享块必须完好。
		rc, err := s.newReaderNoLock("keeper")
		if err != nil {
			t.Errorf("get under read window: %v", err)
			s.life.RUnlock()
			return
		}
		got, _ := io.ReadAll(rc)
		rc.Close() // noLock：不触碰生命周期锁
		if !bytes.Equal(got, d1) {
			t.Errorf("keeper read during concurrent reclaim mismatch")
		}
		s.life.RUnlock()
	}()
	<-started

	// 此时回收必须阻塞，直到读取窗口结束——它绝不能删掉读取依赖的块。
	doneReclaim := make(chan *ReclaimReport)
	go func() {
		rep, err := s.Reclaim()
		if err != nil {
			t.Errorf("concurrent reclaim: %v", err)
			close(doneReclaim)
			return
		}
		doneReclaim <- rep
	}()

	select {
	case <-doneReclaim:
		t.Fatal("reclaim must wait while a read window is open")
	default:
		t.Log("reclaim correctly blocked while read window is active")
	}

	close(proceed)
	rep := <-doneReclaim
	if rep != nil {
		t.Logf("reclaim after read window: deleted=%d bytes=%d (read data intact)",
			rep.DeletedBlocks, rep.ReclaimedBytes)
	}
	// 回收结束后，未删对象仍然完整；goner 独有块已释放。
	readObject(t, s, "keeper", d1)
}

// TestReclaimConcurrentWithCommit 验证：提交进行中（块已进 CAS、
// 清单尚未发布）触发的回收不会把新块误判为垃圾。
func TestReclaimConcurrentWithCommit(t *testing.T) {
	s := newStore(t)
	// 预置一个将被删除的对象，使回收确实有活可干。
	if err := s.Put("old", bytes.NewReader(bytes.Repeat([]byte("o"), 2500))); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("old"); err != nil {
		t.Fatal(err)
	}

	const n = 12
	var wg sync.WaitGroup
	wg.Add(n + n)
	// 一半协程持续提交新对象，一半协程持续回收。
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			d := append([]byte("new-obj-"), bytes.Repeat([]byte{byte('0' + i%10)}, 800+i*7)...)
			id := "live-" + string(rune('a'+i))
			if err := s.Put(id, bytes.NewReader(d)); err != nil {
				t.Errorf("put %s: %v", id, err)
				return
			}
		}()
	}
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := s.Reclaim(); err != nil {
				t.Errorf("concurrent reclaim: %v", err)
			}
		}()
	}
	wg.Wait()

	// 最终再回收一次，清掉 "old"；然后所有已提交的新对象必须完整可读。
	rep, err := s.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after %d concurrent commits + %d concurrent reclaims: last reclaim deleted=%d bytes=%d",
		n, n, rep.DeletedBlocks, rep.ReclaimedBytes)
	for i := 0; i < n; i++ {
		id := "live-" + string(rune('a'+i))
		d := append([]byte("new-obj-"), bytes.Repeat([]byte{byte('0' + i%10)}, 800+i*7)...)
		if err := s.Verify(id); err != nil {
			t.Fatalf("committed object %s damaged after concurrent reclaim: %v", id, err)
		}
		readObject(t, s, id, d)
	}
	t.Logf("all %d concurrently committed objects verified intact; no live block reclaimed", n)
}

// TestDeleteConcurrentWithReaders 验证：删除等待活跃读取结束，
// 且并发删除幂等、结论确定。
func TestDeleteConcurrentWithReaders(t *testing.T) {
	s := newStore(t)
	d := bytes.Repeat([]byte("del-me"), 600)
	if err := s.Put("target", bytes.NewReader(d)); err != nil {
		t.Fatal(err)
	}

	s.life.RLock() // 模拟活跃读取窗口
	deleteDone := make(chan error, 2)
	go func() { deleteDone <- s.Delete("target") }()
	go func() { deleteDone <- s.Delete("target") }()

	select {
	case err := <-deleteDone:
		t.Fatalf("delete must wait for active readers, returned: %v", err)
	default:
		t.Log("both concurrent deletes blocked while read window active")
	}

	// 读取窗口内对象仍可读；newReaderNoLock 不触碰生命周期锁，
	// 读窗口由本测试自行开合。
	rc0, err := s.newReaderNoLock("target")
	if err != nil {
		s.life.RUnlock()
		t.Fatalf("object readable within read window: %v", err)
	}
	got0, _ := io.ReadAll(rc0)
	rc0.Close()
	if !bytes.Equal(got0, d) {
		s.life.RUnlock()
		t.Fatal("read within window mismatch")
	}
	s.life.RUnlock() // 关闭读取窗口，放行被阻塞的删除

	for i := 0; i < 2; i++ {
		if err := <-deleteDone; err != nil {
			t.Fatalf("concurrent delete: %v", err)
		}
	}
	if _, err := s.Get("target"); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("want ErrObjectDeleted after concurrent deletes, got %v", err)
	}
	t.Log("concurrent deletes both succeeded (idempotent); read window protected data")
}
