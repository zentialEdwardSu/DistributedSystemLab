package raft

type Future interface {
	// Error will block until future arrived and return the error
	// of future
	Error() error
}

type IndexFuture interface {
	Future

	// Index will return the log of newly started log
	// call after Error() is called
	Index() int
}

type StartFuture interface {
	IndexFuture

	// Response will return result of log applying
	Response() interface{}
}

// WarpedError helper for generic return type
type WarpedError struct {
	err error
}

func (we WarpedError) Error() error          { return we.err }
func (we WarpedError) Index() int            { return 0 }
func (we WarpedError) Response() interface{} { return nil }

type ErrorFuture struct {
	Arrived    bool          // if the ErrorFuture had arrived
	err        error         // error
	errCh      chan error    // chan to updates self err
	shutdownCh chan struct{} // raft shutdown chan
}

func (e *ErrorFuture) init() {
	e.errCh = make(chan error, 1)
}

func (e *ErrorFuture) Error() error {
	if e.err != nil {
		return e.err
	}
	if e.errCh == nil {
		panic("Wait for closed channel")
	}

	select {
	case err := <-e.errCh:
		return err
	case <-e.shutdownCh:
		return ErrRaftShutdown
	}
}

// ErrorArrived Called when future arrived, error can be nil if success
func (e *ErrorFuture) ErrorArrived(err error) {
	if e.errCh == nil || e.Arrived == true {
		return
	}
	e.errCh <- err
	close(e.errCh)
	e.Arrived = true
}

type LogFuture struct {
	ErrorFuture

	log  LogEntry
	resp interface{}
}

func (lf *LogFuture) Index() int {
	return lf.log.Index
}

func (lf *LogFuture) Response() interface{} {
	return lf.resp
}
