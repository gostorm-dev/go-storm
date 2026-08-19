package transport

import "errors"

var (
	// Configuration errors
	ErrNegativeMaxIdleConns          = errors.New("transport: MaxIdleConns must be non-negative")
	ErrNegativeMaxIdleConnsPerHost   = errors.New("transport: MaxIdleConnsPerHost must be non-negative")
	ErrNegativeIdleConnTimeout       = errors.New("transport: IdleConnTimeout must be non-negative")
	ErrNegativeKeepAlive             = errors.New("transport: KeepAlive must be non-negative")
	ErrNegativeDialTimeout           = errors.New("transport: DialTimeout must be non-negative")
	ErrNegativeTLSHandshakeTimeout   = errors.New("transport: TLSHandshakeTimeout must be non-negative")
	ErrNegativeResponseHeaderTimeout = errors.New("transport: ResponseHeaderTimeout must be non-negative")
	ErrNegativeBufferSize            = errors.New("transport: BufferSize must be non-negative")
)
