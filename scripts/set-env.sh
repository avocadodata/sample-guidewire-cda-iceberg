#!/usr/bin/env bash
# Copyright Amazon.com and its affiliates; all rights reserved.
# SPDX-License-Identifier: MIT-0

# scripts/set-env.sh
# Loads and calls your existing ~/.zshrc version switcher functions
# Usage: source scripts/set-env.sh

if ! command -v setsparkversion >/dev/null 2>&1; then
  [ -f "$HOME/.zshrc" ] && source "$HOME/.zshrc"
fi

setjavaversion 21
setscalaversion 2.13
setsparkversion 4.0

# This will not work until you have setup your bashrc or ~/.zshrc
# with alias to swtich between versions and you have installed already
# in your local machine