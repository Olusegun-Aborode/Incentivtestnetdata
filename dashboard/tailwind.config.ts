import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        background: '#F8F9FB',
        foreground: '#1A1D23',
        card: '#FFFFFF',
        'card-hover': '#F3F4F6',
        'panel-header': '#F1F2F5',
        'text-muted': '#6B7280',
        'accent-orange': '#E55A2B',
        'accent-red': '#DC2626',
        'accent-blue': '#4A6CF7',
        'accent-green': '#059669',
        'accent-purple': '#9333EA',
        'accent-cyan': '#0891B2',
        'accent-yellow': '#D97706',
      },
      borderColor: {
        DEFAULT: 'rgba(0, 0, 0, 0.08)',
        bright: 'rgba(0, 0, 0, 0.15)',
      },
      fontFamily: {
        sans: ['Inter', 'SF Pro Display', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
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
