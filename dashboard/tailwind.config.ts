import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        background: '#0B0D0F',
        foreground: '#E0E0E0',
        card: '#111318',
        'card-hover': '#1A1E24',
        'panel-header': '#0D0F13',
        'text-muted': '#6B7280',
        'accent-orange': '#FF6B35',
        'accent-red': '#FF4444',
        'accent-blue': '#5B7FFF',
        'accent-green': '#10B981',
        'accent-purple': '#B44AFF',
        'accent-cyan': '#00D4FF',
        'accent-yellow': '#F59E0B',
      },
      borderColor: {
        DEFAULT: 'rgba(255, 255, 255, 0.08)',
        bright: 'rgba(255, 255, 255, 0.15)',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'SF Mono', 'Consolas', 'monospace'],
      },
      keyframes: {
        pulse: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.4' },
        },
        shimmer: {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
      },
      animation: {
        pulse: 'pulse 2s ease-in-out infinite',
        shimmer: 'shimmer 1.5s ease-in-out infinite',
      },
    },
  },
  plugins: [],
};

export default config;
