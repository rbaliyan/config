package config_test

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func ExampleNew() {
	ctx := context.Background()

	// Create a manager with an in-memory store
	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Connect to the backend
	if err := mgr.Connect(ctx); err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer mgr.Close(ctx)

	// Get a namespaced config and set a value
	cfg := mgr.Namespace("production")
	if err := cfg.Set(ctx, "app/timeout", 30); err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Read the value back
	val, err := cfg.Get(ctx, "app/timeout")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	i, _ := val.Int64()
	fmt.Println("timeout:", i)
	// Output: timeout: 30
}

func ExampleConfig_Get() {
	ctx := context.Background()

	mgr, _ := config.New(config.WithStore(memory.NewStore()))
	mgr.Connect(ctx)
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("app")
	cfg.Set(ctx, "feature/dark-mode", true)

	val, err := cfg.Get(ctx, "feature/dark-mode")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	enabled, _ := val.Bool()
	fmt.Println("dark mode:", enabled)
	// Output: dark mode: true
}

func ExampleNewValue() {
	// Create a value and inspect its type
	val := config.NewValue(42)
	fmt.Println("type:", val.Type())

	i, err := val.Int64()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("value:", i)
	// Output:
	// type: int
	// value: 42
}

func ExampleContextWithManager() {
	ctx := context.Background()

	mgr, _ := config.New(config.WithStore(memory.NewStore()))
	mgr.Connect(ctx)
	defer mgr.Close(ctx)

	// Add manager and namespace to context
	ctx = config.ContextWithManager(ctx, mgr)
	ctx = config.ContextWithNamespace(ctx, "myapp")

	// Set and get via context convenience functions
	config.Set(ctx, "greeting", "hello")

	val, _ := config.Get(ctx, "greeting")
	s, _ := val.String()
	fmt.Println(s)
	// Output: hello
}
