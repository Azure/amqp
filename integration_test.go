package amqp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

var localBrokerAddr string
var rng *rand.Rand

func init() {
	// rand used to generate queue names, non-determinism is fine for this use
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	localBrokerAddr = os.Getenv("AMQP_BROKER_ADDR")
}

type lockedError struct {
	mu  sync.RWMutex
	err error
}

func (l *lockedError) read() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (l *lockedError) write(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil {
		l.err = err
	}
}

func TestIntegrationRoundTrip(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	tests := []struct {
		label    string
		sessions uint16
		data     []string
	}{
		{
			label:    "1 roundtrip, small payload",
			sessions: 1,
			data:     []string{"1Hello there!"},
		},
		{
			label:    "3 roundtrip, small payload",
			sessions: 1,
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
		{
			label:    "1000 roundtrip, small payload",
			sessions: 1,
			data: repeatStrings(1000,
				"3Hey there!",
				"3Hi there!",
				"3Ho there!",
			),
		},
		{
			label:    "1 roundtrip, small payload, 10 sessions",
			sessions: 10,
			data:     []string{"1Hello there!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, &amqp.ConnOptions{
				MaxSessions: tt.sessions,
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			for i := uint16(0); i < tt.sessions; i++ {
				// Open a session
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				session, err := client.NewSession(ctx, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// add a random suffix to the link name so the test broker always creates a new node
				targetName := fmt.Sprintf("%s %d", tt.label, rng.Uint64())

				// Create a sender
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(ctx, targetName, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// Perform test concurrently for speed and to catch races
				wg := &sync.WaitGroup{}
				wg.Add(2)

				sendErr := lockedError{}
				go func() {
					defer wg.Done()
					maxSendSemaphore := make(chan struct{}, 20)
					for i, d := range tt.data {
						if sendErr.read() != nil {
							// don't send anymore messages if there was an error
							return
						}
						maxSendSemaphore <- struct{}{}
						go func(index int, data string) {
							defer func() { <-maxSendSemaphore }()
							msg := amqp.NewMessage([]byte(data))
							msg.ApplicationProperties = make(map[string]any)
							msg.ApplicationProperties["i"] = index
							ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
							err := sender.Send(ctx, msg, nil)
							cancel()
							if err != nil {
								sendErr.write(fmt.Errorf("error after %d sends: %+v", index, err))
							}
						}(i, d)
					}
				}()

				receiveErr := lockedError{}
				receiveCount := 0
				go func() {
					defer wg.Done()

					// Create a receiver
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
						Credit: 10,
					})
					cancel()
					if err != nil {
						receiveErr.write(err)
						return
					}
					defer testClose(t, receiver.Close)

					for i := 0; i < len(tt.data); i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
						msg, err := receiver.Receive(ctx, nil)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("error after %d receives: %+v", i, err))
							break
						}
						ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
						err = receiver.AcceptMessage(ctx, msg)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("failed to accept message: %v", err))
							break
						}
						if msg.DeliveryTag == nil {
							receiveErr.write(fmt.Errorf("error after %d receives: nil deliverytag received", i))
							break
						}
						msgIndex, ok := msg.ApplicationProperties["i"].(int64)
						if !ok {
							receiveErr.write(fmt.Errorf("failed to parse i. %v", msg.ApplicationProperties["i"]))
							break
						}
						expectedData := tt.data[msgIndex]
						if !bytes.Equal([]byte(expectedData), msg.GetData()) {
							receiveErr.write(fmt.Errorf("expected received message %d to be %v, but it was %v", msgIndex, expectedData, string(msg.GetData())))
							break
						}
						receiveCount++
					}
				}()

				wg.Wait()
				testClose(t, sender.Close)
				if err = sendErr.read(); err != nil {
					t.Fatalf("send error: %v", err)
				}
				if err = receiveErr.read(); err != nil {
					t.Fatalf("receive error: %v", err)
				}
				if expected := len(tt.data); receiveCount != expected {
					t.Fatalf("expected %d got %d", expected, receiveCount)
				}
			}

			client.Close() // close before leak check
			checkLeaks()   // this is done here because queuesClient starts additional goroutines
		})
	}
}

func TestIntegrationRoundTrip_Buffered(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	tests := []struct {
		label string
		data  []string
	}{
		{
			label: "10 buffer, small payload",
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			// add a random suffix to the link name so the test broker always creates a new node
			targetName := fmt.Sprintf("%s %d", tt.label, rng.Uint64())
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			sender, err := session.NewSender(ctx, targetName, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				err = sender.Send(ctx, amqp.NewMessage([]byte(data)), nil)
				cancel()
				if err != nil {
					t.Fatalf("Error after %d sends: %+v", i, err)
				}
			}
			testClose(t, sender.Close)

			// Create a receiver
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
				Credit:                    int32(len(tt.data)),
				RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// read buffered messages
			for i, data := range tt.data {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				msg, err := receiver.Receive(ctx, nil)
				cancel()
				if err != nil {
					t.Fatalf("Error after %d receives: %+v", i, err)
				}
				if !bytes.Equal([]byte(data), msg.GetData()) {
					t.Fatalf("Expected received message %d to be %v, but it was %v", i+1, string(data), string(msg.GetData()))
				}
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				err = receiver.AcceptMessage(ctx, msg)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}

			// close link
			testClose(t, receiver.Close)
			client.Close() // close before leak check
			checkLeaks()   // this is done here because queuesClient starts additional goroutines
		})
	}
}

func TestIntegrationReceiverModeSecond(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}
	tests := []struct {
		label    string
		sessions int
		data     []string
	}{
		{
			label:    "3 roundtrip, small payload",
			sessions: 1,
			data: []string{
				"2Hey there!",
				"2Hi there!",
				"2Ho there!",
			},
		},
		{
			label:    "1000 roundtrip, small payload",
			sessions: 1,
			data: repeatStrings(1000,
				"3Hey there!",
				"3Hi there!",
				"3Ho there!",
			),
		},
		{
			label:    "1 roundtrip, small payload, 10 sessions",
			sessions: 10,
			data:     []string{"1Hello there!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			for i := 0; i < tt.sessions; i++ {
				// Open a session
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				session, err := client.NewSession(ctx, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// add a random suffix to the link name so the test broker always creates a new node
				targetName := fmt.Sprintf("%s %d", tt.label, rng.Uint64())

				// Create a sender
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(ctx, targetName, nil)
				cancel()
				if err != nil {
					t.Fatal(err)
				}

				// Perform test concurrently for speed and to catch races
				var wg sync.WaitGroup
				wg.Add(2)

				sendErr := lockedError{}
				go func() {
					defer wg.Done()
					defer testClose(t, sender.Close)

					for i, data := range tt.data {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						err = sender.Send(ctx, amqp.NewMessage([]byte(data)), nil)
						cancel()
						if err != nil {
							sendErr.write(fmt.Errorf("Error after %d sends: %+v", i, err))
							break
						}
					}
				}()

				receiveErr := lockedError{}
				go func() {
					defer wg.Done()

					// Create a receiver
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
						SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
					})
					cancel()
					if err != nil {
						receiveErr.write(err)
						return
					}
					defer testClose(t, receiver.Close)

					for i, data := range tt.data {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						msg, err := receiver.Receive(ctx, nil)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("Error after %d receives: %+v", i, err))
							break
						}
						if !bytes.Equal([]byte(data), msg.GetData()) {
							receiveErr.write(fmt.Errorf("Expected received message %d to be %v, but it was %v", i+1, string(data), string(msg.GetData())))
							break
						}
						ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
						err = receiver.AcceptMessage(ctx, msg)
						cancel()
						if err != nil {
							receiveErr.write(fmt.Errorf("Error accepting message: %+v", err))
							break
						}
					}
				}()

				wg.Wait()

				if err = sendErr.read(); err != nil {
					t.Fatalf("send error: %v", err)
				}
				if err = receiveErr.read(); err != nil {
					t.Fatalf("receive error: %v", err)
				}
			}

			client.Close() // close before leak check
			checkLeaks()   // this is done here because queuesClient starts additional goroutines
		})
	}
}

func TestIntegrationSessionHandleMax(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	tests := []struct {
		maxLinks uint32
		links    int
		close    int
		error    *regexp.Regexp
	}{
		{
			maxLinks: 4,
			links:    5,
			error:    regexp.MustCompile(`handle max \(4\)`),
		},
		{
			maxLinks: 5,
			links:    5,
		},
		{
			maxLinks: 4,
			links:    5,
			close:    1,
		},
		{
			maxLinks: 4,
			links:    8,
			close:    4,
		},
		{
			maxLinks: 62,
			links:    64,
			close:    2,
		},
		{
			maxLinks: 62,
			links:    64,
			close:    1,
			error:    regexp.MustCompile(`handle max \(62\)`),
		},
	}

	for _, tt := range tests {
		label := fmt.Sprintf("max %d, links %d, close %d", tt.maxLinks, tt.links, tt.close)
		t.Run(label, func(t *testing.T) {
			// checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, &amqp.SessionOptions{
				MaxLinks: tt.maxLinks,
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			var matches int

			// Create a sender
			for i := 0; i < tt.links; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				sender, err := session.NewSender(ctx, fmt.Sprintf("TestIntegrationSessionHandleMax %d", rng.Uint64()), nil)
				cancel()
				switch {
				case err == nil:
				case tt.error == nil:
					t.Fatal(err)
				case !tt.error.MatchString(err.Error()):
					t.Errorf("expect error to match %q, but it was %q", tt.error, err)
				default:
					matches++
				}

				if tt.close > 0 {
					err = sender.Close(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					tt.close--
				}
			}

			if tt.error != nil && matches == 0 {
				t.Errorf("expect an error")
			}
			if tt.error != nil && matches > 1 {
				t.Errorf("expected 1 matching error, got %d", matches)
			}
		})
	}
}

func TestIntegrationLinkName(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	tests := []struct {
		name  string
		error string
	}{
		{
			name:  "linkA",
			error: "link with name 'linkA' already exists",
		},
	}

	for _, tt := range tests {
		label := fmt.Sprintf("name %v", tt.name)
		t.Run(label, func(t *testing.T) {
			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			senderOrigin, err := session.NewSender(ctx, "TestIntegrationLinkName", &amqp.SenderOptions{
				Name: tt.name,
			})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, senderOrigin.Close)

			// This one should fail
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			sender, err := session.NewSender(ctx, "TestIntegrationLinkName", &amqp.SenderOptions{
				Name: tt.name,
			})
			cancel()
			if err == nil {
				testClose(t, sender.Close)
			}

			switch {
			case err == nil && tt.error == "":
				// success
			case err == nil:
				t.Fatalf("expected error to contain %q, but it was nil", tt.error)
			case !strings.Contains(err.Error(), tt.error):
				t.Errorf("expected error to contain %q, but it was %q", tt.error, err)
			}
		})
	}
}

func TestIntegrationClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	for times := 0; times < 100; times++ {
		label := "link"
		t.Run(label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			receiver, err := session.NewReceiver(ctx, "TestIntegrationClose", nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			testClose(t, receiver.Close)

			_, err = receiver.Receive(context.Background(), nil)
			var linkErr *amqp.LinkError
			require.ErrorAs(t, err, &linkErr)

			err = client.Close() // close before leak check
			if err != nil {
				t.Fatal(err)
			}

			checkLeaks()
		})

		label = "session"
		t.Run(label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			receiver, err := session.NewReceiver(ctx, "TestIntegrationClose", nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			testClose(t, session.Close)

			msg, err := receiver.Receive(context.Background(), nil)
			var sessionErr *amqp.SessionError
			require.ErrorAs(t, err, &sessionErr)
			if msg != nil {
				t.Fatal("expected nil message")
			}

			err = client.Close() // close before leak check
			if err != nil {
				t.Fatal(err)
			}

			checkLeaks()
		})

		label = "conn"
		t.Run(label, func(t *testing.T) {
			checkLeaks := leaktest.CheckTimeout(t, 60*time.Second)

			// Create client
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			client, err := amqp.Dial(ctx, localBrokerAddr, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Open a session
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			// Create a sender
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			receiver, err := session.NewReceiver(ctx, "TestIntegrationClose", nil)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			err = client.Close()
			if err != nil {
				t.Fatalf("Expected nil error from client.Close(), got: %+v", err)
			}

			msg, err := receiver.Receive(context.Background(), nil)
			var connErr *amqp.ConnError
			if !errors.As(err, &connErr) {
				t.Fatalf("unexpected error type %T", err)
				return
			}
			if msg != nil {
				t.Fatal("expected nil message")
			}

			checkLeaks()
		})
	}
}

func TestMultipleSessionsOpenClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	sessions := [10]*amqp.Session{}
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Fatalf("failed to create session: %v", err)
				return
			}
			sessions[j] = session
		}
		for _, session := range sessions {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = session.Close(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	client.Close()
	checkLeaks()
}

func TestDialWithCancelledContext(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	var dialErr *net.OpError
	require.ErrorAs(t, err, &dialErr)
	require.Nil(t, client)

	checkLeaks()
}

func TestNewSessionWithCancelledContext(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	// we use an iteration greater than the max number of sessions to ensure
	// that no begin performatives are being sent. if they are then the peer
	// will reject the session with a different error.
	for times := 0; times < 10000; times++ {
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		session, err := client.NewSession(ctx, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, session)
	}

	require.NoError(t, client.Close())
	checkLeaks()
}

func TestConcurrentSessionsOpenClose(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			session, err := client.NewSession(ctx, nil)
			cancel()
			if err != nil {
				t.Errorf("failed to create session: %v", err)
				return
			}
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			err = session.Close(ctx)
			cancel()
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	client.Close()
	checkLeaks()
}

func TestReceiverModeFirst(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, "TestReceiverModeFirst", nil)
	cancel()
	require.NoError(t, err)

	const (
		linkCredit = 10
		msgCount   = 2 * linkCredit
	)

	// prime with a bunch of messages
	for i := 0; i < msgCount; i++ {
		err = sender.Send(context.Background(), amqp.NewMessage([]byte(fmt.Sprintf("test %d", i))), nil)
		require.NoError(t, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = sender.Close(ctx)
	cancel()
	require.NoError(t, err)

	// create a new sender
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err = session.NewSender(ctx, "TestReceiverModeFirstOther", nil)
	cancel()
	require.NoError(t, err)

	// now drain the messages

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	recv, err := session.NewReceiver(ctx, "TestReceiverModeFirst", &amqp.ReceiverOptions{
		Credit: linkCredit,
	})
	cancel()
	require.NoError(t, err)

	msgs := make(chan *amqp.Message, linkCredit)
	for i := 0; i < msgCount; i++ {
		// receive one message
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		msg, err := recv.Receive(ctx, nil)
		cancel()
		require.NoError(t, err)

		msgs <- msg

		// send to other sender
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = sender.Send(ctx, msg, nil)
		cancel()
		require.NoError(t, err)

		if (i+1)%linkCredit == 0 {
			for j := 0; j < linkCredit; j++ {
				msg = <-msgs
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				err = recv.AcceptMessage(ctx, msg)
				cancel()
				require.NoError(t, err)
			}
		}
	}

	client.Close()
	checkLeaks()
}

func TestSenderExactlyOnce(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, "TestSenderExactlyOnce", &amqp.SenderOptions{
		SettlementMode:              amqp.SenderSettleModeUnsettled.Ptr(),
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = sender.Send(ctx, amqp.NewMessage([]byte("hello!")), nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	receiver, err := session.NewReceiver(ctx, "TestSenderExactlyOnce", &amqp.ReceiverOptions{
		SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	msg, err := receiver.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)

	require.Equal(t, "hello!", string(msg.GetData()))
	client.Close()
	checkLeaks()
}

func TestReceivingLotsOfSettledMessages(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	// Open a session
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	// Create a sender
	// add a random suffix to the link name so the test broker always creates a new node
	targetName := fmt.Sprintf("TestReceivingLotsOfSettledMessages %d", rng.Uint64())
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, targetName, nil)
	cancel()
	require.NoError(t, err)

	const linkCredit = 10

	// send more messages than the receiver's link credit
	const msgCount = linkCredit * 2
	for i := 0; i < msgCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err = sender.Send(ctx, amqp.NewMessage([]byte(fmt.Sprintf("TestReceivingLotsOfSettledMessages %d", i))), nil)
		cancel()
		require.NoError(t, err)
	}
	testClose(t, sender.Close)

	// Create a receiver
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	receiver, err := session.NewReceiver(ctx, targetName, &amqp.ReceiverOptions{
		Credit:                    linkCredit,
		RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	var receivedCount int
	for i := 0; i < msgCount; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		msg, err := receiver.Receive(ctx, nil)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		require.EqualValues(t, fmt.Sprintf("TestReceivingLotsOfSettledMessages %d", i), string(msg.GetData()))
		receivedCount++
	}

	// now we received all the messages
	require.EqualValues(t, msgCount, receivedCount)

	testClose(t, receiver.Close)

	client.Close()
}

func TestNewReceiverWithCancelledContext(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	// Open a session
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	for times := 0; times < 100; times++ {
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		receiver, err := session.NewReceiver(ctx, "thesource", nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, receiver)
	}

	require.NoError(t, client.Close())
	checkLeaks()
}

func TestMultipleSendersSharedSession(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, &amqp.ConnOptions{
		SASLType: amqp.SASLTypeAnonymous(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	const msgSize = 1024 * 10
	bigMsgData := make([]byte, msgSize)
	for i := 0; i < msgSize; i++ {
		bigMsgData[i] = byte(i % 256)
	}

	wg := &sync.WaitGroup{}
	for senders := 0; senders < 100; senders++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msgCount := 0; msgCount < 50; msgCount++ {
				sender, err := session.NewSender(context.Background(), "TestMultipleSendersSharedSession", nil)
				require.NoError(t, err)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = sender.Send(ctx, amqp.NewMessage(bigMsgData), nil)
				cancel()
				if err != nil {
					panic(err)
				}
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
				err = sender.Close(ctx)
				if err != nil {
					panic(err)
				}
				cancel()
				time.Sleep(time.Millisecond * 100)
			}
		}()
	}

	wg.Wait()
	require.NoError(t, client.Close())
	checkLeaks()
}

func TestDrainingLink(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	queue := fmt.Sprintf("TestDrainingLink-%d", rand.Int63())

	conn, err := amqp.Dial(context.Background(), localBrokerAddr, nil)
	require.NoError(t, err)

	session, err := conn.NewSession(context.Background(), nil)
	require.NoError(t, err)

	receiver, err := session.NewReceiver(context.Background(), queue, &amqp.ReceiverOptions{
		Credit:         -1,
		SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	require.NoError(t, err)

	defer func() {
		err := receiver.Close(context.Background())
		require.NoError(t, err)
	}()

	// we'll send one message right now
	send := func(count int) {
		sender, err := session.NewSender(context.Background(), queue, nil)
		require.NoError(t, err)

		defer func() {
			err := sender.Close(context.Background())
			require.NoError(t, err)
		}()

		data := make([]byte, 1000)

		for i := 0; i < count; i++ {
			err = sender.Send(context.Background(), &amqp.Message{
				Value: data,
			}, nil)
			require.NoError(t, err)
		}
	}

	const totalSent = 1

	// Send one message, but ask for two. This guarantees that we'll leave one active credit after
	// we receive our single message.
	send(totalSent)
	err = receiver.IssueCredit(totalSent + 1) // request 2 messages, even though 1 is available. This will leave us with 1 extra credit.
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 1, len(receiveAllPrefetched(receiver)), "we received the single message that was available")

	// now we'll drain - there's a single active credit that will now get thrown away
	err = receiver.DrainCredit(context.Background())
	require.NoError(t, err)
	require.Zero(t, receiveAllPrefetched(receiver)) // there weren't any messages to send to us

	// Our receiver should have _zero_ active credits at this point
	// because we've completed drain. We'll send a message _but_ since we have no
	// active credits nothing will be sent to _our_ receiver.
	send(1)
	time.Sleep(200 * time.Millisecond)

	// we haven't issued any credits so we _shouldn't_ get anything here unless there's a bug in
	// our receiver, or in the broker.
	require.Empty(t, receiveAllPrefetched(receiver))
}

func TestSenderNullValue(t *testing.T) {
	if localBrokerAddr == "" {
		t.Skip()
	}

	checkLeaks := leaktest.Check(t)

	// Create client
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := amqp.Dial(ctx, localBrokerAddr, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	sender, err := session.NewSender(ctx, "TestSenderNullValue", &amqp.SenderOptions{
		SettlementMode:              amqp.SenderSettleModeUnsettled.Ptr(),
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	err = sender.Send(ctx, &amqp.Message{Value: amqp.Null{}}, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	receiver, err := session.NewReceiver(ctx, "TestSenderNullValue", &amqp.ReceiverOptions{
		SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	msg, err := receiver.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)

	require.Nil(t, msg.Value)
	client.Close()
	checkLeaks()
}

func repeatStrings(count int, strs ...string) []string {
	var out []string
	for i := 0; i < count; i += len(strs) {
		out = append(out, strs...)
	}
	return out[:count]
}

func testClose(t testing.TB, close func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := close(ctx)
	if err != nil {
		t.Errorf("error closing: %+v\n", err)
	}
}

func receiveAllPrefetched(receiver *amqp.Receiver) []*amqp.Message {
	var messages []*amqp.Message

	for {
		if msg := receiver.Prefetched(); msg == nil {
			break
		} else {
			messages = append(messages, msg)
		}
	}

	return messages
}
