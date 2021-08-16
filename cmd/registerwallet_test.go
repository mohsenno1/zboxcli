package cmd

import (
	"bytes"
	"io"
	"log"
	"os"

	"github.com/0chain/gosdk/core/zcncrypto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
)

var _ = Describe("Registerwallet", func() {
	var (
		registerCommand *cobra.Command
	)
	Describe("Registering a wallet", func() {
		Context("with ./zbox register command", func() {
			It("should output Wallet registered", func() {
				// Capture stdout
				old := os.Stdout // keep backup of the real stdout
				r, w, err := os.Pipe()
				if err != nil {
					log.Fatal(err)
				}
				os.Stdout = w

				outC := make(chan string)
				// copy the output in a separate goroutine so printing can't block indefinitely
				go func() {
					var buf bytes.Buffer
					io.Copy(&buf, r)
					outC <- buf.String()
				}()

				clientWallet = &zcncrypto.Wallet{}
				registerCommand = registerWalletCmd
				registerCommand.Run(registerCommand, []string{})

				// back to normal state
				w.Close()
				os.Stdout = old // restoring the real stdout
				out := <-outC

				Expect(out).To(Equal("Wallet registered\n"))
			})
		})
	})
})
