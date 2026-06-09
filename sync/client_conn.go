package sync

import (
	"context"
	"errors"
	"strconv"
	"time"

	sync "github.com/testground/sync-service"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func (c *DefaultClient) nextID() (id string) {
	c.nextMu.Lock()
	id = strconv.Itoa(c.next)
	c.next++
	c.nextMu.Unlock()
	return id
}

func (c *DefaultClient) responsesWorker() {
	for {
		res, err := c.readSocket()
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(c.ctx.Err(), context.Canceled) ||
				websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				break
			}

			c.log.Fatalw("error while reading socket", "error", err)
		}

		var h *pendingRequest
		c.handlersMu.Lock()
		h = c.handlers[res.ID]
		c.handlersMu.Unlock()

		if h == nil {
			c.log.Warnf("no handler available for response: %s", res.ID)
		} else {
			// Deliver, or drop if the request was torn down. h.ch is never closed,
			// so this can never panic on send-to-closed; h.done guarantees we never
			// block forever on a receiver that has gone away.
			select {
			case h.ch <- res:
			case <-h.done:
			}
		}
	}

	c.wg.Done()
}

func (c *DefaultClient) makeRequest(ctx context.Context, req *sync.Request) (chan *sync.Response, error) {
	if c.ctx.Err() != nil {
		return nil, errors.New("tried to make request after context being cancelled")
	}

	if req.ID == "" {
		req.ID = c.nextID()
	}

	ch := make(chan *sync.Response)
	done := make(chan struct{})

	c.handlersMu.Lock()
	c.handlers[req.ID] = &pendingRequest{ch: ch, done: done}
	c.handlersMu.Unlock()

	err := c.writeSocket(req)
	if err != nil {
		return nil, err
	}

	c.wg.Add(1)

	go func() {
		// Wait for either of the contexts to fire.
		select {
		case <-c.ctx.Done():
		case <-ctx.Done():
		}

		c.handlersMu.Lock()
		delete(c.handlers, req.ID)
		c.handlersMu.Unlock()
		close(done) // tell responsesWorker to stop; ch is intentionally NOT closed
		c.wg.Done()
	}()

	return ch, nil
}

// awaitResponse waits for the single response to a one-shot request, or for the
// request or client context to be cancelled. The handler channel is never closed,
// so the context is the only unblock.
func (c *DefaultClient) awaitResponse(ctx context.Context, ch chan *sync.Response) (*sync.Response, error) {
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errors.New("client closed before getting response")
	}
}

func (c *DefaultClient) readSocket() (*sync.Response, error) {
	// After one hour without receiving information from the sync service,
	// the test will inevitably fail. Note(hacdias): consider changing
	// the timeout to a larger value in case slower tests fail. The same
	// value must be changed on the sync service side too.
	ctx, cancel := context.WithTimeout(c.ctx, time.Hour)
	defer cancel()

	var req *sync.Response
	err := wsjson.Read(ctx, c.socket, &req)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errors.New("received nil from socket")
	}
	return req, err
}

func (c *DefaultClient) writeSocket(req *sync.Request) error {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second)
	defer cancel()
	return wsjson.Write(ctx, c.socket, req)
}
