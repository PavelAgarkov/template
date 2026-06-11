package service

import (
	rateenvelopequeue "github.com/simplegear/rate-envelope-queue"
)

type (
	Orchestrator interface {
		Start()
		Stop()
	}
)

type OrchestratorPool struct {
	orchestrators []rateenvelopequeue.SingleQueuePool
	start         []func()
}

func NewOrchestratorPool(start []func()) Orchestrator {
	return &OrchestratorPool{
		start: start,
	}
}

func (o *OrchestratorPool) Start() {
	for _, startFunc := range o.start {
		startFunc()
	}
}

func (o *OrchestratorPool) Stop() {
}
