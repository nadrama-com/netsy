// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"reflect"

	"github.com/spf13/viper"
)

// init viper, set defaults, and bind env vars using the runtimeConfig struct
func init() {
	// Set defaults, process flags, and bind env vars
	typeOf := reflect.TypeOf(runtimeConfig{})
	for i := range typeOf.NumField() {
		field := typeOf.Field(i)
		viperKey, ok := field.Tag.Lookup("viper")
		if !ok {
			panic("Unexpected missing viper tag on Config struct")
		}
		// set default
		if defaultValue, ok := field.Tag.Lookup("default"); ok {
			viper.SetDefault(viperKey, defaultValue)
		}
		// bind env
		if envkey, ok := field.Tag.Lookup("envkey"); ok {
			viper.BindEnv(viperKey, envkey)
		}
	}
	// Auto convert strings to appropriate types (like "true" to boolean)
	viper.AutomaticEnv()
}
