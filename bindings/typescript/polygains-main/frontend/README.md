# PolyGains Frontend

React-based web dashboard for PolyGains analytics platform. Built with Bun, Vite, and Tailwind CSS.

## Tech Stack

- **Runtime**: [Bun](https://bun.sh) - Fast JavaScript runtime & bundler
- **Framework**: React 18 with JSX
- **Styling**: Tailwind CSS + DaisyUI components
- **Build**: Custom Bun build script with plugin-tailwind
- **Icons**: Lucide React

## Development

```bash
# Install dependencies
bun install

# Start development server (with HMR)
bun dev
```

The dev server runs on `http://localhost:3000` by default.

### API Proxy

Frontend requests to `api/*` are proxied to the backend server:

```bash
# Set upstream API URL
export API_UPSTREAM_BASE_URL=http://127.0.0.1:4000

# Or use .env file
echo "API_UPSTREAM_BASE_URL=http://127.0.0.1:4000" > .env
```

## Production Build

```bash
# Build to ../public/dist/
bun run build.ts

# With options
bun run build.ts --minify --sourcemap=linked

# Custom output directory
bun run build.ts --outdir=/var/www/html
```

## Project Structure

```
frontend/
├── src/
│   ├── main.jsx        # App entry point
│   ├── components/     # React components
│   ├── hooks/          # Custom hooks
│   └── styles/         # CSS/styling
├── build.ts            # Bun build script
├── index.html          # HTML template
└── package.json
```

## Favicon & Meta Tags

The `index.html` includes optimized favicon and social meta tags:

- **Favicons**: ICO, PNG, and WebP formats for all sizes
- **Apple Touch**: 180×180 icon for iOS
- **Android**: 192×192 and 512×512 icons
- **Social**: Open Graph and Twitter card images

These static assets are served from the parent `public/` directory.

## Build Configuration

The build script (`build.ts`) supports all Bun build options:

```bash
bun run build.ts --help
```

Common options:
- `--outdir <path>` - Output directory
- `--minify` - Enable minification
- `--sourcemap <type>` - Sourcemap type
- `--target <target>` - Build target
- `--splitting` - Code splitting

Default build config:
- Output: `../public/dist/`
- Minification: enabled
- Target: browser
- Sourcemaps: linked
