package interstate

import "time"

type updaterOptions struct {
	waitForLock     bool
	waitTimeout     *time.Duration
	pollingInterval time.Duration
}

type updaterOptionsFn func(o *updaterOptions)

func WithWaitForLock() updaterOptionsFn {
	return func(o *updaterOptions) {
		o.waitForLock = true
	}
}

func WithWaitTimeout(v time.Duration) updaterOptionsFn {
	return func(o *updaterOptions) {
		o.waitTimeout = &v
	}
}

func WithPollingInterval(v time.Duration) updaterOptionsFn {
	return func(o *updaterOptions) {
		o.pollingInterval = v
	}
}
