// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build !requirefips

package beatcmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/elastic/beats/v7/libbeat/common/cli"
	"github.com/elastic/beats/v7/libbeat/common/terminal"
	"github.com/elastic/elastic-agent-libs/keystore"
)

var keystoreCommand = &cobra.Command{
	Use:   "keystore",
	Short: "Manage secrets keystore",
}

func init() {
	keystoreCommand.AddCommand(genCreateKeystoreCmd())
	keystoreCommand.AddCommand(genAddKeystoreCmd())
	keystoreCommand.AddCommand(genRemoveKeystoreCmd())
	keystoreCommand.AddCommand(genListKeystoreCmd())
}

func genCreateKeystoreCmd() *cobra.Command {
	var flagForce bool
	command := &cobra.Command{
		Use:   "create",
		Short: "Create keystore",
		Run: cli.RunWith(func(cmd *cobra.Command, args []string) error {
			return createKeystore(flagForce)
		}),
	}
	command.Flags().BoolVar(&flagForce, "force", false, "override the existing keystore")
	return command
}

func genAddKeystoreCmd() *cobra.Command {
	var flagForce bool
	var flagStdin bool
	command := &cobra.Command{
		Use:   "add",
		Short: "Add secret",
		Run: cli.RunWith(func(cmd *cobra.Command, args []string) error {
			store, err := getKeystore()
			if err != nil {
				return err
			}
			return addKey(store, args, flagForce, flagStdin)
		}),
	}
	command.Flags().BoolVar(&flagStdin, "stdin", false, "Use the stdin as the source of the secret")
	command.Flags().BoolVar(&flagForce, "force", false, "Override the existing key")
	return command
}

func genRemoveKeystoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove",
		Short: "Remove secret",
		Run: cli.RunWith(func(cmd *cobra.Command, args []string) error {
			store, err := getKeystore()
			if err != nil {
				return err
			}
			return removeKey(store, args)
		}),
	}
}

func genListKeystoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List keystore",
		Run: cli.RunWith(func(cmd *cobra.Command, args []string) error {
			store, err := getKeystore()
			if err != nil {
				return err
			}
			return list(store)
		}),
	}
}

func createKeystore(force bool) error {
	store, err := getKeystore()
	if err != nil {
		return err
	}

	writableKeystore, err := keystore.AsWritableKeystore(store)
	if err != nil {
		return fmt.Errorf("error creating the keystore: %w", err)
	}

	if store.IsPersisted() && !force {
		overwrite := terminal.PromptYesNo("A keystore already exists, Overwrite?", false)
		if overwrite {
			err := writableKeystore.Create(true)
			if err != nil {
				return fmt.Errorf("error creating the keystore: %w", err)
			}
		} else {
			fmt.Println("Exiting without creating keystore.")
			return nil
		}
	} else {
		err := writableKeystore.Create(true)
		if err != nil {
			return fmt.Errorf("error creating the keystore: %w", err)
		}
	}
	fmt.Printf("Created apm-server keystore\n")
	return nil
}

func getKeystore() (keystore.Keystore, error) {
	_, _, keystore, err := LoadConfig()
	return keystore, err
}

func addKey(store keystore.Keystore, keys []string, force, stdin bool) error {
	if len(keys) == 0 {
		return errors.New("failed to create the secret: no key provided")
	}

	if len(keys) > 1 {
		return fmt.Errorf("could not create secret for: %s, you can only provide one key per invocation", keys)
	}

	writableKeystore, err := keystore.AsWritableKeystore(store)
	if err != nil {
		return fmt.Errorf("error creating the keystore: %w", err)
	}

	if !store.IsPersisted() {
		if !force {
			create := terminal.PromptYesNo("The keystore does not exist. Do you want to create it?", false)
			if !create {
				return errors.New("exiting without creating keystore")
			}
		}
		err := writableKeystore.Create(true)
		if err != nil {
			return fmt.Errorf("could not create keystore, error: %w", err)
		}
		fmt.Println("Created keystore")
	}

	key := strings.TrimSpace(keys[0])
	if _, err := store.Retrieve(key); err == nil {
		if !force {
			if stdin {
				return fmt.Errorf("the settings %s already exist in the keystore use `--force` to replace it", key)
			}
			overwrite := terminal.PromptYesNo(fmt.Sprintf("Setting %s already exists, Overwrite?", key), false)
			if !overwrite {
				fmt.Println("Exiting without modifying keystore.")
				return nil
			}
		}
	} else if !errors.Is(err, keystore.ErrKeyDoesntExists) {
		return err
	}

	var keyValue []byte
	if stdin {
		reader := bufio.NewReader(os.Stdin)
		keyValue, err = io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("could not read input from stdin")
		}
	} else {
		fmt.Printf("Enter value for %s: ", key)
		keyValue, err = term.ReadPassword(int(syscall.Stdin))
		fmt.Println()
		if err != nil {
			return fmt.Errorf("could not read value from the input, error: %w", err)
		}
	}
	if err = writableKeystore.Store(key, keyValue); err != nil {
		return fmt.Errorf("could not add the key in the keystore, error: %w", err)
	}
	if err = writableKeystore.Save(); err != nil {
		return fmt.Errorf("fail to save the keystore: %w", err)
	} else {
		fmt.Println("Successfully updated the keystore")
	}
	return nil
}

func removeKey(store keystore.Keystore, keys []string) error {
	if len(keys) == 0 {
		return errors.New("you must supply at least one key to remove")
	}

	writableKeystore, err := keystore.AsWritableKeystore(store)
	if err != nil {
		return fmt.Errorf("error deleting the keystore: %w", err)
	}

	if !store.IsPersisted() {
		return errors.New("the keystore doesn't exist. Use the 'create' command to create one")
	}

	for _, key := range keys {
		key = strings.TrimSpace(key)
		_, err := store.Retrieve(key)
		if err != nil {
			return fmt.Errorf("could not find key '%v' in the keystore", key)
		}

		writableKeystore.Delete(key)
		err = writableKeystore.Save()
		if err != nil {
			return fmt.Errorf("could not update the keystore with the changes, key: %s, error: %w", key, err)
		}
		fmt.Printf("successfully removed key: %s\n", key)
	}
	return nil
}

func list(store keystore.Keystore) error {
	listingKeystore, err := keystore.AsListingKeystore(store)
	if err != nil {
		return fmt.Errorf("error listing the keystore: %w", err)
	}
	keys, err := listingKeystore.List()
	if err != nil {
		return fmt.Errorf("could not read values from the keystore, error: %w", err)
	}
	for _, key := range keys {
		fmt.Println(key)
	}
	return nil
}
