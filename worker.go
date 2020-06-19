package gpool

// Worker Worker
type Worker interface {
	Do(func())
}

// WorkerFactory WorkerFactory
type WorkerFactory func(<-chan struct{}, <-chan func(), func()) Worker

type worker struct {
	doneCh    <-chan struct{}
	taskCh    chan func()
	taskQueue <-chan func()
	onDone    func()
}

func defaultWorkerFactory(doneCh <-chan struct{}, taskQueue <-chan func(), onDone func()) Worker {
	w := &worker{
		taskCh:    make(chan func(), 1),
		taskQueue: taskQueue,
		doneCh:    doneCh,
		onDone:    onDone,
	}
	go w.loop()
	return w
}

func (w *worker) Do(task func()) {
	w.taskCh <- task
}

func (w *worker) loop() {
	defer w.onDone()
	for {
		select {
		case task := <-w.taskCh:
			task()
		case task := <-w.taskQueue:
			task()
		case <-w.doneCh:
			return
		}
	}
}
