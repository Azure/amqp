package amqp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/debug"
	"github.com/Azure/go-amqp/internal/encoding"
	"github.com/Azure/go-amqp/internal/frames"
	"github.com/Azure/go-amqp/internal/shared"
)

// Sender sends messages on a single AMQP link.
type Sender struct {
	l         link
	transfers chan frames.PerformTransfer // sender uses to send transfer frames

	// Indicates whether we should automatically close the link on disposition errors or not.
	// Some AMQP servers (like Event Hubs) benefit from keeping the link open on disposition errors
	// (for instance, if you're doing many parallel sends over the same link and you get back a
	// throttling error, which is not fatal)
	closeOnDispositionError bool

	mu              sync.Mutex // protects buf and nextDeliveryTag
	buf             buffer.Buffer
	nextDeliveryTag uint64

	// The number of messages awaiting credit at the link sender endpoint. Only the sender can independently
	// set this value. The receiver sets this to the last known value seen from the sender.
	availableCredit uint32
}

// LinkName() is the name of the link used for this Sender.
func (s *Sender) LinkName() string {
	return s.l.key.name
}

// MaxMessageSize is the maximum size of a single message.
func (s *Sender) MaxMessageSize() uint64 {
	return s.l.maxMessageSize
}

// SendOptions contains any optional values for the Sender.Send method.
type SendOptions struct {
	// for future expansion
}

// Send sends a Message.
//
// Blocks until the message is sent, ctx completes, or an error occurs.
//
// Send is safe for concurrent use. Since only a single message can be
// sent on a link at a time, this is most useful when settlement confirmation
// has been requested (receiver settle mode is "Second"). In this case,
// additional messages can be sent while the current goroutine is waiting
// for the confirmation.
func (s *Sender) Send(ctx context.Context, msg *Message, opts *SendOptions) error {
	// check if the link is dead.  while it's safe to call s.send
	// in this case, this will avoid some allocations etc.
	select {
	case <-s.l.done:
		return s.l.doneErr
	default:
		// link is still active
	}
	done, err := s.send(ctx, msg)
	if err != nil {
		return err
	}

	// wait for transfer to be confirmed
	select {
	case state := <-done:
		if state, ok := state.(*encoding.StateRejected); ok {
			if s.detachOnRejectDisp() {
				// TODO: this appears to be duplicated in the mux
				return &LinkError{RemoteErr: state.Error}
			}
			return state.Error
		}
		return nil
	case <-s.l.done:
		return s.l.doneErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// send is separated from Send so that the mutex unlock can be deferred without
// locking the transfer confirmation that happens in Send.
func (s *Sender) send(ctx context.Context, msg *Message) (chan encoding.DeliveryState, error) {
	const (
		maxDeliveryTagLength   = 32
		maxTransferFrameHeader = 66 // determined by calcMaxTransferFrameHeader
	)
	if len(msg.DeliveryTag) > maxDeliveryTagLength {
		return nil, fmt.Errorf("delivery tag is over the allowed %v bytes, len: %v", maxDeliveryTagLength, len(msg.DeliveryTag))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf.Reset()
	err := msg.Marshal(&s.buf)
	if err != nil {
		return nil, err
	}

	if s.l.maxMessageSize != 0 && uint64(s.buf.Len()) > s.l.maxMessageSize {
		return nil, fmt.Errorf("encoded message size exceeds max of %d", s.l.maxMessageSize)
	}

	var (
		maxPayloadSize = int64(s.l.session.conn.peerMaxFrameSize) - maxTransferFrameHeader
		sndSettleMode  = s.l.senderSettleMode
		senderSettled  = sndSettleMode != nil && (*sndSettleMode == SenderSettleModeSettled || (*sndSettleMode == SenderSettleModeMixed && msg.SendSettled))
	)

	deliveryTag := msg.DeliveryTag
	if len(deliveryTag) == 0 {
		// use uint64 encoded as []byte as deliveryTag
		deliveryTag = make([]byte, 8)
		binary.BigEndian.PutUint64(deliveryTag, s.nextDeliveryTag)
		s.nextDeliveryTag++
	}

	fr := frames.PerformTransfer{
		Handle:        s.l.handle,
		DeliveryID:    &needsDeliveryID,
		DeliveryTag:   deliveryTag,
		MessageFormat: &msg.Format,
		More:          s.buf.Len() > 0,
	}

	for fr.More {
		buf, _ := s.buf.Next(maxPayloadSize)
		fr.Payload = append([]byte(nil), buf...)
		fr.More = s.buf.Len() > 0
		if !fr.More {
			// SSM=settled: overrides RSM; no acks.
			// SSM=unsettled: sender should wait for receiver to ack
			// RSM=first: receiver considers it settled immediately, but must still send ack (SSM=unsettled only)
			// RSM=second: receiver sends ack and waits for return ack from sender (SSM=unsettled only)

			// mark final transfer as settled when sender mode is settled
			fr.Settled = senderSettled

			// set done on last frame
			fr.Done = make(chan encoding.DeliveryState, 1)
		}

		select {
		case s.transfers <- fr:
		case <-s.l.done:
			return nil, s.l.doneErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		// clear values that are only required on first message
		fr.DeliveryID = nil
		fr.DeliveryTag = nil
		fr.MessageFormat = nil
	}

	return fr.Done, nil
}

// Address returns the link's address.
func (s *Sender) Address() string {
	if s.l.target == nil {
		return ""
	}
	return s.l.target.Address
}

// Close closes the Sender and AMQP link.
func (s *Sender) Close(ctx context.Context) error {
	return s.l.closeLink(ctx)
}

// newSendingLink creates a new sending link and attaches it to the session
func newSender(target string, session *Session, opts *SenderOptions) (*Sender, error) {
	s := &Sender{
		l: link{
			key:     linkKey{shared.RandString(40), encoding.RoleSender},
			session: session,
			close:   make(chan struct{}),
			done:    make(chan struct{}),
			target:  &frames.Target{Address: target},
			source:  new(frames.Source),
		},
		closeOnDispositionError: true,
	}

	if opts == nil {
		return s, nil
	}

	for _, v := range opts.Capabilities {
		s.l.source.Capabilities = append(s.l.source.Capabilities, encoding.Symbol(v))
	}
	if opts.Durability > DurabilityUnsettledState {
		return nil, fmt.Errorf("invalid Durability %d", opts.Durability)
	}
	s.l.source.Durable = opts.Durability
	if opts.DynamicAddress {
		s.l.target.Address = ""
		s.l.dynamicAddr = opts.DynamicAddress
	}
	if opts.ExpiryPolicy != "" {
		if err := encoding.ValidateExpiryPolicy(opts.ExpiryPolicy); err != nil {
			return nil, err
		}
		s.l.source.ExpiryPolicy = opts.ExpiryPolicy
	}
	s.l.source.Timeout = opts.ExpiryTimeout
	s.closeOnDispositionError = !opts.IgnoreDispositionErrors
	if opts.Name != "" {
		s.l.key.name = opts.Name
	}
	if opts.Properties != nil {
		s.l.properties = make(map[encoding.Symbol]any)
		for k, v := range opts.Properties {
			if k == "" {
				return nil, errors.New("link property key must not be empty")
			}
			s.l.properties[encoding.Symbol(k)] = v
		}
	}
	if opts.RequestedReceiverSettleMode != nil {
		if rsm := *opts.RequestedReceiverSettleMode; rsm > ReceiverSettleModeSecond {
			return nil, fmt.Errorf("invalid RequestedReceiverSettleMode %d", rsm)
		}
		s.l.receiverSettleMode = opts.RequestedReceiverSettleMode
	}
	if opts.SettlementMode != nil {
		if ssm := *opts.SettlementMode; ssm > SenderSettleModeMixed {
			return nil, fmt.Errorf("invalid SettlementMode %d", ssm)
		}
		s.l.senderSettleMode = opts.SettlementMode
	}
	s.l.source.Address = opts.SourceAddress
	for _, v := range opts.TargetCapabilities {
		s.l.target.Capabilities = append(s.l.target.Capabilities, encoding.Symbol(v))
	}
	if opts.TargetDurability != DurabilityNone {
		s.l.target.Durable = opts.TargetDurability
	}
	if opts.TargetExpiryPolicy != ExpiryPolicySessionEnd {
		s.l.target.ExpiryPolicy = opts.TargetExpiryPolicy
	}
	if opts.TargetExpiryTimeout != 0 {
		s.l.target.Timeout = opts.TargetExpiryTimeout
	}
	return s, nil
}

func (s *Sender) attach(ctx context.Context) error {
	s.l.rx = make(chan frames.FrameBody, 1)

	if err := s.l.attach(ctx, func(pa *frames.PerformAttach) {
		pa.Role = encoding.RoleSender
		if pa.Target == nil {
			pa.Target = new(frames.Target)
		}
		pa.Target.Dynamic = s.l.dynamicAddr
	}, func(pa *frames.PerformAttach) {
		if s.l.target == nil {
			s.l.target = new(frames.Target)
		}

		// if dynamic address requested, copy assigned name to address
		if s.l.dynamicAddr && pa.Target != nil {
			s.l.target.Address = pa.Target.Address
		}
	}); err != nil {
		return err
	}

	s.transfers = make(chan frames.PerformTransfer)

	go s.mux()

	return nil
}

func (s *Sender) mux() {
	defer s.l.muxClose(context.Background(), nil, nil, nil)

	// used to track and send disposition frames.
	// frames are sent in FIFO order.
	outgoingDisp := make(chan *frames.PerformDisposition, 1)
	outgoingDisps := []*frames.PerformDisposition{}

Loop:
	for {
		var outgoingTransfers chan frames.PerformTransfer
		if s.availableCredit > 0 {
			debug.Log(1, "TX (Sender) (enable): target: %q, available credit: %d, deliveryCount: %d", s.l.target.Address, s.availableCredit, s.l.deliveryCount)
			outgoingTransfers = s.transfers
		} else {
			debug.Log(1, "TX (Sender) (pause): target: %q, available credit: %d, deliveryCount: %d", s.l.target.Address, s.availableCredit, s.l.deliveryCount)
		}

		if len(outgoingDisps) > 0 && len(outgoingDisp) == 0 {
			// queue up the next outgoing frame and remove it from the slice
			outgoingDisp <- outgoingDisps[0]
			outgoingDisps = outgoingDisps[1:]
		}

		handleFrame := func(fr frames.FrameBody) error {
			var disp *frames.PerformDisposition
			disp, s.l.doneErr = s.muxHandleFrame(fr)
			if s.l.doneErr != nil {
				return s.l.doneErr
			} else if disp != nil {
				outgoingDisps = append(outgoingDisps, disp)
			}
			return nil
		}

		select {
		case dr := <-outgoingDisp:
			// Ensure the session mux is not blocked
			for {
				select {
				case s.l.session.tx <- dr:
					debug.Log(2, "TX (Sender): mux frame to Session: %d, %s", s.l.session.channel, dr)
					continue Loop
				case fr := <-s.l.rx:
					if err := handleFrame(fr); err != nil {
						return
					}
				case <-s.l.close:
					continue Loop
				case <-s.l.session.done:
					continue Loop
				}
			}

		// received frame
		case fr := <-s.l.rx:
			if err := handleFrame(fr); err != nil {
				return
			}

		// send data
		case tr := <-outgoingTransfers:
			// Ensure the session mux is not blocked
			for {
				select {
				case s.l.session.txTransfer <- &tr:
					debug.Log(2, "TX (Sender): mux transfer to Session: %d, %s", s.l.session.channel, tr)
					// decrement link-credit after entire message transferred
					if !tr.More {
						s.l.deliveryCount++
						s.availableCredit--
						// we are the sender and we keep track of the peer's link credit
						debug.Log(3, "TX (Sender): link: %s, available credit: %d", s.l.key.name, s.availableCredit)
					}
					continue Loop
				case fr := <-s.l.rx:
					if err := handleFrame(fr); err != nil {
						return
					}
				case <-s.l.close:
					continue Loop
				case <-s.l.session.done:
					continue Loop
				}
			}

		case <-s.l.close:
			s.l.doneErr = &LinkError{}
			return
		case <-s.l.session.done:
			// TODO: per spec, if the session has terminated, we're not allowed to send frames
			s.l.doneErr = s.l.session.doneErr
			return
		}
	}
}

// muxHandleFrame processes fr based on type.
// depending on the peer's RSM, it might return a disposition frame for sending
func (s *Sender) muxHandleFrame(fr frames.FrameBody) (*frames.PerformDisposition, error) {
	debug.Log(2, "RX (Sender): %s", fr)
	switch fr := fr.(type) {
	// flow control frame
	case *frames.PerformFlow:
		linkCredit := *fr.LinkCredit - s.l.deliveryCount
		if fr.DeliveryCount != nil {
			// DeliveryCount can be nil if the receiver hasn't processed
			// the attach. That shouldn't be the case here, but it's
			// what ActiveMQ does.
			linkCredit += *fr.DeliveryCount
		}
		// TODO: clean up as part of flow control fixes
		s.availableCredit = linkCredit

		if !fr.Echo {
			return nil, nil
		}

		var (
			// copy because sent by pointer below; prevent race
			deliveryCount = s.l.deliveryCount
		)

		// send flow
		// TODO: missing Available and session info
		resp := &frames.PerformFlow{
			Handle:        &s.l.handle,
			DeliveryCount: &deliveryCount,
			LinkCredit:    &linkCredit, // max number of messages
		}
		_ = s.l.session.txFrame(resp, nil)

	case *frames.PerformDisposition:
		// If sending async and a message is rejected, cause a link error.
		//
		// This isn't ideal, but there isn't a clear better way to handle it.
		if fr, ok := fr.State.(*encoding.StateRejected); ok && s.detachOnRejectDisp() {
			return nil, &LinkError{RemoteErr: fr.Error}
		}

		if fr.Settled {
			return nil, nil
		}

		// peer is in mode second, so we must send confirmation of disposition.
		// NOTE: the ack must be sent through the session so it can close out
		// the in-flight disposition.
		return &frames.PerformDisposition{
			Role:    encoding.RoleSender,
			First:   fr.First,
			Last:    fr.Last,
			Settled: true,
		}, nil

	default:
		return nil, s.l.muxHandleFrame(fr)
	}

	return nil, nil
}

func (s *Sender) detachOnRejectDisp() bool {
	// only detach on rejection when no RSM was requested or in ModeFirst.
	// if the receiver is in ModeSecond, it will send an explicit rejection disposition
	// that we'll have to ack. so in that case, we don't treat it as a link error.
	if s.closeOnDispositionError && (s.l.receiverSettleMode == nil || *s.l.receiverSettleMode == ReceiverSettleModeFirst) {
		return true
	}
	return false
}
