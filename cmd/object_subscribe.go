//go:build client

/***************************************************************
 *
 * Copyright (C) 2026, Pelican Project, Morgridge Institute for Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/pelicanplatform/pelican/client"
	"github.com/pelicanplatform/pelican/config"
	"github.com/pelicanplatform/pelican/param"
)

var (
	subscribeCmd = &cobra.Command{
		Use:   "subscribe {object}",
		Short: "Listen for notifications of new and modified objects under a path",
		Long: `Subscribe to notifications of objects created or modified under the given path, and print each as it arrives.

The director is asked which origin can send notifications about the path; only origins that advertise the Notify capability are considered.
The command connects to the notification endpoint of the best of them and prints the path of each object it announces, one per line, until interrupted.
Objects that already existed when you subscribed are not announced, and neither are deletions.
The origin checks for changes about once a minute, so a notification can be that late.

The depth is how many levels below the path you want to hear about:
objects directly in the path are one level down, those in its subdirectories two, and so on.

Each subscription has a client id, your own name for it, which is also the first part of the topic subscribed to.
The origin keeps the notifications you have not yet received under that id, so running the command again with the same --client-id picks up where you left off.
If you do not pick one with --client-id, a random one is made.
Use --verbose to see it, along with the endpoint and topic, before listening starts.

If the path is not publicly readable, the origin requires a token with read access to it, found the same way as for other object commands:
the file given with --token, otherwise the usual environment variables and locations, otherwise one acquired from the namespace's issuer.
For a public path a token is sent only if one is given or found.

Output goes to standard output, one notification per line; details and status go to standard error.

With --no-listen the command asks the director and prints the endpoint, the origins and the topic to subscribe to, then exits without connecting.
That is on standard output, as JSON with --json.`,
		Args: cobra.ExactArgs(1),
		RunE: subscribeMain,
	}
)

func init() {
	subscribeCmd.Flags().StringP("token", "t", "", "Token file to use to subscribe")
	subscribeCmd.Flags().BoolP("json", "j", false, "Print each notification as a JSON object")
	subscribeCmd.Flags().Int("depth", 1, "How many levels below the path to hear about; objects directly in the path are one level down")
	subscribeCmd.Flags().String("client-id", "", "Name for the subscription, the first part of the topic (default: a random id)")
	subscribeCmd.Flags().BoolP("verbose", "v", false, "Before listening, print the connection endpoint and subscription parameters")
	subscribeCmd.Flags().Bool("no-listen", false, "Print the endpoint, origins and topic, and exit without connecting")
	objectCmd.AddCommand(subscribeCmd)
}

func subscribeMain(cmd *cobra.Command, args []string) error {
	if err := config.InitClient(); err != nil {
		log.Errorln(err)
		os.Exit(1)
	}

	depth, _ := cmd.Flags().GetInt("depth")
	clientID, _ := cmd.Flags().GetString("client-id")
	verbose, _ := cmd.Flags().GetBool("verbose")
	asJSON, _ := cmd.Flags().GetBool("json")
	noListen, _ := cmd.Flags().GetBool("no-listen")
	tokenLocation, _ := cmd.Flags().GetString("token")

	result, err := client.DoSubscribe(cmd.Context(), args[0], depth, clientID, client.WithTokenLocation(tokenLocation))
	if err != nil {
		return errors.Wrapf(err, "failed to subscribe to %s", args[0])
	}

	verifyTLS := !param.TLSSkipVerify.GetBool()
	if noListen {
		return writeSubscription(os.Stdout, result, asJSON, verifyTLS)
	}
	if verbose {
		writeSubscriptionDetails(os.Stderr, result, verifyTLS)
		fmt.Fprintln(os.Stderr, "Listening; press Ctrl-C to stop")
	} else {
		fmt.Fprintf(os.Stderr, "Listening for notifications at %s; press Ctrl-C to stop\n", result.NotifyUrl)
	}

	// Listen until the user has had enough.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return listenForNotifications(ctx, result, os.Stdout, asJSON)
}

// writeSubscription prints a subscription's details, as text or as JSON, for a
// caller that wants them without listening.
func writeSubscription(w io.Writer, result client.SubscribeResult, asJSON, verifyTLS bool) error {
	if !asJSON {
		writeSubscriptionDetails(w, result, verifyTLS)
		return nil
	}
	jsonData, err := json.Marshal(result)
	if err != nil {
		return errors.Wrap(err, "failed to marshal subscription info to JSON format")
	}
	_, err = fmt.Fprintln(w, string(jsonData))
	return err
}

// writeSubscriptionDetails says where a subscription will connect and what it will
// ask for: the endpoint, the origins the director chose among, and the parameters
// the origin will receive.
func writeSubscriptionDetails(w io.Writer, result client.SubscribeResult, verifyTLS bool) {
	// Everything after the client id is what the origin sees of the subscription.
	_, params, _ := strings.Cut(result.Topic, "/")

	fmt.Fprintf(w, "Namespace:           %s\n", result.Namespace)
	fmt.Fprintf(w, "Origins:             %s\n", strings.Join(result.Origins, ", "))
	fmt.Fprintf(w, "Endpoint:            %s\n", result.NotifyUrl)
	fmt.Fprintf(w, "TLS verification:    %t\n", verifyTLS)
	fmt.Fprintf(w, "Client ID:           %s\n", result.ClientID)
	fmt.Fprintf(w, "Depth:               %d\n", result.Depth)
	fmt.Fprintf(w, "Topic:               %s\n", result.Topic)
	fmt.Fprintf(w, "Subscription params: %s\n", params)
	fmt.Fprintf(w, "Acknowledgement:     client-individual\n")
}

// listenForNotifications prints each notification the origin sends to w until ctx
// is done or the subscription ends.
func listenForNotifications(ctx context.Context, result client.SubscribeResult, w io.Writer, asJSON bool) error {
	return result.Listen(ctx, func(n client.Notification) {
		if err := writeNotification(w, n, asJSON); err != nil {
			log.Errorf("Failed to print a notification: %v", err)
		}
	})
}

// writeNotification prints one notification, as its path or as a JSON object.
func writeNotification(w io.Writer, n client.Notification, asJSON bool) error {
	if !asJSON {
		_, err := fmt.Fprintln(w, n.Path)
		return err
	}
	jsonData, err := json.Marshal(struct {
		Path       string `json:"path"`
		ReceivedAt string `json:"receivedAt"`
	}{n.Path, n.ReceivedAt.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(jsonData))
	return err
}
