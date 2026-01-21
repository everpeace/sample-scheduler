package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"

	"github.com/everpeace/sample-scheduler/pkg/scheduler"
)

// schedulerCmd represents the secondary-scheduler command when called without any subcommands
var schedulerCmd = &cobra.Command{
	Use: "secondary-scheduler",
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()

		klog.Info("Starting secondary-scheduler...")
		config, err := rest.InClusterConfig()
		if err != nil {
			config, err = clientcmd.BuildConfigFromFlags("", homedir.HomeDir()+"/.kube/config")
			if err != nil {
				panic(err.Error())
			}
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err.Error())
		}

		scheduler := scheduler.NewScheduler(clientset, time.Minute*10)
		if err := scheduler.Start(ctx); err != nil {
			panic(err.Error())
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := schedulerCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.secondary-scheduler.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
}
