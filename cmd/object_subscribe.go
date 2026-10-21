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
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/pelicanplatform/pelican/client"
	"github.com/pelicanplatform/pelican/config"
)

var (
	subscribeCmd = &cobra.Command{
		Use:   "subscribe {object}",
		Short: "Find the origins that can notify you of new objects under a path",
		Long: `Ask the federation's director which origins can send notifications about objects
created or modified under the given path.

Only origins that advertise the Notify capability are considered.
The command prints the notification endpoint, the origins the director redirected to,
and the topic to subscribe to for the path and depth you asked for.
It does not itself open a notification connection.

The depth is how many levels below the path you want to hear about:
objects directly in the path are one level down, those in its subdirectories two, and so on.

The topic begins with a client id, your own name for the subscription.
Send it as the "subscription" header of the STOMP SUBSCRIBE, and use the same id to resume a subscription later.
If you do not pick one with --client-id, a random one is made and printed.`,
		Args: cobra.ExactArgs(1),
		RunE: subscribeMain,
	}
)

func init() {
	subscribeCmd.Flags().BoolP("json", "j", false, "Print results in JSON format")
	subscribeCmd.Flags().Int("depth", 1, "How many levels below the path to hear about; objects directly in the path are one level down")
	subscribeCmd.Flags().String("client-id", "", "Name for the subscription, the first part of the topic (default: a random id)")
	objectCmd.AddCommand(subscribeCmd)
}

func subscribeMain(cmd *cobra.Command, args []string) error {
	if err := config.InitClient(); err != nil {
		log.Errorln(err)
		os.Exit(1)
	}

	depth, _ := cmd.Flags().GetInt("depth")
	clientID, _ := cmd.Flags().GetString("client-id")

	result, err := client.DoSubscribe(cmd.Context(), args[0], depth, clientID)
	if err != nil {
		return errors.Wrapf(err, "failed to subscribe to %s", args[0])
	}

	if asJSON, _ := cmd.Flags().GetBool("json"); asJSON {
		jsonData, err := json.Marshal(result)
		if err != nil {
			return errors.Wrap(err, "failed to marshal subscription info to JSON format")
		}
		fmt.Println(string(jsonData))
		return nil
	}

	fmt.Printf("Namespace:    %s\n", result.Namespace)
	fmt.Printf("Notify URL:   %s\n", result.NotifyUrl)
	fmt.Printf("Token needed: %t\n", result.RequireToken)
	fmt.Println("Origins:")
	for _, origin := range result.Origins {
		fmt.Printf("  %s\n", origin)
	}
	fmt.Printf("Client ID:    %s\n", result.ClientID)
	fmt.Printf("Depth:        %d\n", result.Depth)
	fmt.Printf("Topic:        %s\n", result.Topic)
	return nil
}
