#!/bin/bash

# Setup GitHub SSH keys for github.com/mev-tools/polygains

set -e

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
KEY_PATH="$REPO_DIR/keys/github_key"
PUB_KEY_PATH="$REPO_DIR/keys/github_key.pub"

echo "Setting up GitHub SSH keys..."

# Ensure keys exist
if [ ! -f "$KEY_PATH" ] || [ ! -f "$PUB_KEY_PATH" ]; then
    echo "Error: SSH keys not found at $KEY_PATH"
    exit 1
fi

# Create ~/.ssh directory if it doesn't exist
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

# Copy keys to ~/.ssh
cp "$KEY_PATH" "$HOME/.ssh/github_key_polygains"
cp "$PUB_KEY_PATH" "$HOME/.ssh/github_key_polygains.pub"

# Set correct permissions on private key
chmod 600 "$HOME/.ssh/github_key_polygains"
chmod 644 "$HOME/.ssh/github_key_polygains.pub"
echo "✓ Copied SSH keys to ~/.ssh/"

# Configure Git for this repository
cd "$REPO_DIR"

# Configure git user for this repo
read -p "Enter your Git name: " GIT_NAME
read -p "Enter your Git email: " GIT_EMAIL

git config user.name "$GIT_NAME"
git config user.email "$GIT_EMAIL"
echo "✓ Configured git user.name and user.email for this repo"

# Check if remote exists, if not add it
if ! git remote get-url origin &>/dev/null; then
    echo "Adding origin remote..."
    git remote add origin git@github.com:mev-tools/polygains.git
else
    # Update existing remote to use SSH
    git remote set-url origin git@github.com:mev-tools/polygains.git
fi
echo "✓ Configured remote: $(git remote get-url origin)"

# Configure Git to use the specific SSH key
git config core.sshCommand "ssh -i $HOME/.ssh/github_key_polygains -o IdentitiesOnly=yes"
echo "✓ Configured Git to use SSH key"

# Test the connection
echo ""
echo "Testing SSH connection to GitHub..."
if ssh -i "$HOME/.ssh/github_key_polygains" -T git@github.com 2>&1 | grep -q "successfully authenticated"; then
    echo "✓ SSH connection successful!"
else
    echo "⚠ SSH authentication result (this is expected for GitHub)"
fi

echo ""
echo "✓ GitHub setup complete!"
echo ""
echo "Repository configured with:"
echo "  User: $GIT_NAME <$GIT_EMAIL>"
echo "  Remote: $(git remote get-url origin)"
echo "  SSH Key: ~/.ssh/github_key_polygains"
echo ""
echo "You can now use git commands:"
echo "  git fetch"
echo "  git pull"
echo "  git push"
