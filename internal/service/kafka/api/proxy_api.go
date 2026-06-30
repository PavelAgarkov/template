package api

import (
	"io"
	"log"
	"net/http"

	"github.com/PavelAgarkov/template/pkg/kafka"

	"github.com/PavelAgarkov/template/pkg/kafka/proxy_loader"
	"github.com/PavelAgarkov/template/pkg/metrics"

	"github.com/valyala/bytebufferpool"
)

// Размер чанка при чтении тела HTTP-запроса (32 KiB)
// const readChunkSize = 32 * 1024
// const readChunkSize = 128 * 1024
const readChunkSize = 256 * 1024

type ProxyAPI struct {
	proxyLoader proxy_loader.ProxyLoader
	metrics     *metrics.Metrics

	byteBufferShkPool      *bytebufferpool.Pool
	byteTempBufferShkPool  *bytebufferpool.Pool
	byteBufferTarePool     *bytebufferpool.Pool
	byteTempBufferTarePool *bytebufferpool.Pool
}

func NewProxyAPI(metrics *metrics.Metrics, proxyLoader proxy_loader.ProxyLoader) Proxy {
	p := &ProxyAPI{
		proxyLoader:            proxyLoader,
		metrics:                metrics,
		byteBufferShkPool:      &bytebufferpool.Pool{},
		byteBufferTarePool:     &bytebufferpool.Pool{},
		byteTempBufferShkPool:  &bytebufferpool.Pool{},
		byteTempBufferTarePool: &bytebufferpool.Pool{},
	}

	return p
}

func (p *ProxyAPI) ReceiveShkOnPlaceBytesBufferStreamV1(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	bodyByteBuffer := p.byteBufferShkPool.Get()
	defer func() {
		// Не возвращаем гигантские слайсы в пул
		if cap(bodyByteBuffer.B) > kafka.MaxKeepBytes {
			bodyByteBuffer.B = nil
		}
		p.byteBufferShkPool.Put(bodyByteBuffer)
	}()

	tempByteBuffer := p.byteTempBufferShkPool.Get()
	if cap(tempByteBuffer.B) < readChunkSize {
		tempByteBuffer.B = make([]byte, readChunkSize)
	}
	tempByteBuffer.B = tempByteBuffer.B[:readChunkSize]
	defer p.byteTempBufferShkPool.Put(tempByteBuffer)

	// Копируем тело в bodyBB, используя tmpBB.B как io.Copy буфер
	_, err := io.CopyBuffer(bodyByteBuffer, r.Body, tempByteBuffer.B)
	if err != nil && err != io.EOF {
		log.Printf("read body error: %v", err)
		return
	}

	_, _ = w.Write([]byte("OK"))
}

func (p *ProxyAPI) ReceiveTareMoveBytesBufferStreamV1(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	bodyByteBuffer := p.byteBufferTarePool.Get()
	defer func() {
		// не возвращаем гигантские backing arrays в пул
		if cap(bodyByteBuffer.B) > kafka.MaxKeepBytes {
			bodyByteBuffer.B = nil
		}
		p.byteBufferTarePool.Put(bodyByteBuffer)
	}()

	tempByteBuffer := p.byteTempBufferTarePool.Get()
	if cap(tempByteBuffer.B) < readChunkSize {
		tempByteBuffer.B = make([]byte, readChunkSize)
	}
	tempByteBuffer.B = tempByteBuffer.B[:readChunkSize]
	defer p.byteTempBufferTarePool.Put(tempByteBuffer)

	// читаем тело целиком в bodyBB через наш чанк
	_, err := io.CopyBuffer(bodyByteBuffer, r.Body, tempByteBuffer.B)
	if err != nil && err != io.EOF {
		log.Printf("read body error: %v", err)
		return
	}

	_, _ = w.Write([]byte("OK"))
}
