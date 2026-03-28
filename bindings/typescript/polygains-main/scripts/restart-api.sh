#!/bin/bash
set -e

echo "🔧 Rebuilding API server..."
bun build --target=bun --outdir=dist src/services/server.ts

echo "🔄 Restarting API service..."
bunx pm2 restart polygains-api || echo "API not running, will start with make start"

echo "✅ API server restarted!"
echo ""
echo "📊 Checking status..."
sleep 2
bunx pm2 status
