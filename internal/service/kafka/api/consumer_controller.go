package api

import (
	"encoding/json"
	"net/http"

	rateenvelopequeue "github.com/simplegear/rate-envelope-queue"
)

type ConsumerControllerInterface interface {
	Controller() http.Handler
}

type ConsumerController struct {
	goodsConsumerPool        rateenvelopequeue.SingleQueuePool
	restartGoodsConsumerPool func()
	tareConsumerPool         rateenvelopequeue.SingleQueuePool
	restartTareConsumerPool  func()
}

func NewConsumerController(goodsConsumerPool, tareConsumerPool rateenvelopequeue.SingleQueuePool, restartGoodsConsumerPool, restartTareConsumerPool func()) ConsumerControllerInterface {
	return &ConsumerController{
		goodsConsumerPool:        goodsConsumerPool,
		restartGoodsConsumerPool: restartGoodsConsumerPool,
		tareConsumerPool:         tareConsumerPool,
		restartTareConsumerPool:  restartTareConsumerPool,
	}
}

func (c *ConsumerController) Controller() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Action string `json:"action"`
			Name   string `json:"name"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		switch body.Action {
		case "start":
			c.start(body.Name)
		case "stop":
			c.stop(body.Name)
		default:
			http.Error(w, "unknown action", http.StatusBadRequest)
		}
	})
}

func (c *ConsumerController) start(name string) {
	switch name {
	case "goods":
		c.goodsConsumerPool.Start()
		c.restartGoodsConsumerPool()
	case "tare":
		c.tareConsumerPool.Start()
		c.restartTareConsumerPool()
	default:
		return
	}
}

func (c *ConsumerController) stop(name string) {
	switch name {
	case "goods":
		c.goodsConsumerPool.Stop()
	case "tare":
		c.tareConsumerPool.Stop()
	default:
		return
	}
}
