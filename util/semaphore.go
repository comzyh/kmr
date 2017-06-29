package util

var empty4sem struct{}

type Semaphore struct {
	sem chan struct{}
}

func NewSemaphore(n int) *Semaphore {
	s := &Semaphore{
		sem: make(chan struct{}, n),
	}
	for i := 0; i < n; i++ {
		s.sem <- empty4sem
	}
	return s
}

func (s *Semaphore) Acquire(n int) {
	for i := 0; i < n; i++ {
		<-s.sem
	}
}

func (s *Semaphore) Release(n int) {
	for i := 0; i < n; i++ {
		s.sem <- empty4sem
	}
}
