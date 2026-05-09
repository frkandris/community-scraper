/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./scraper/web/templates/**/*.html",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
        mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      colors: {
        // Warm sand neutrals (replaces slate)
        sand: {
          50:  '#FAFAF8',
          100: '#F5F2EC',
          200: '#EAE5DB',
          300: '#D8D1C4',
          400: '#B5ADA0',
          500: '#8C8478',
          600: '#6A6259',
          700: '#4A4441',
          800: '#2E2B29',
          900: '#1C1917',
          950: '#110F0E',
        },
        // Terracotta accent (replaces emerald)
        terra: {
          50:  '#FDF0EA',
          100: '#FAD9C7',
          200: '#F3B49A',
          300: '#E88E6B',
          400: '#DC6F45',
          500: '#C2613A',
          600: '#A8512F',
          700: '#8A4226',
          800: '#6B341E',
          900: '#4C2615',
          950: '#2E160B',
        },
        // Warm amber — status/warning
        amber: {
          50:  '#FFFBEB',
          100: '#FEF3C7',
          200: '#FDE68A',
          300: '#FCD34D',
          400: '#FBBF24',
          500: '#F59E0B',
          600: '#D97706',
          700: '#B45309',
          800: '#92400E',
          900: '#78350F',
        },
        // Keep a minimal green for positive/success states
        moss: {
          50:  '#F0FDF4',
          100: '#DCFCE7',
          500: '#22C55E',
          600: '#16A34A',
          700: '#15803D',
        },
      },
      fontSize: {
        'xs':   ['11px', { lineHeight: '1.5', letterSpacing: '0.01em' }],
        'sm':   ['13px', { lineHeight: '1.5', letterSpacing: '0.005em' }],
        'base': ['15px', { lineHeight: '1.6' }],
        'md':   ['16px', { lineHeight: '1.6' }],
        'lg':   ['18px', { lineHeight: '1.5' }],
        'xl':   ['20px', { lineHeight: '1.4' }],
        '2xl':  ['24px', { lineHeight: '1.3', letterSpacing: '-0.01em' }],
        '3xl':  ['30px', { lineHeight: '1.2', letterSpacing: '-0.015em' }],
        '4xl':  ['36px', { lineHeight: '1.15', letterSpacing: '-0.02em' }],
        '5xl':  ['48px', { lineHeight: '1.1',  letterSpacing: '-0.025em' }],
      },
      spacing: {
        '18': '4.5rem',
        '22': '5.5rem',
        '30': '7.5rem',
      },
      borderRadius: {
        'sm':  '4px',
        'DEFAULT': '6px',
        'md':  '8px',
        'lg':  '12px',
        'xl':  '16px',
        '2xl': '24px',
      },
      boxShadow: {
        'sm':    '0 1px 2px 0 rgb(28 25 23 / 0.05)',
        'DEFAULT': '0 1px 3px 0 rgb(28 25 23 / 0.08), 0 1px 2px -1px rgb(28 25 23 / 0.06)',
        'md':    '0 4px 6px -1px rgb(28 25 23 / 0.08), 0 2px 4px -2px rgb(28 25 23 / 0.05)',
        'lg':    '0 10px 15px -3px rgb(28 25 23 / 0.08), 0 4px 6px -4px rgb(28 25 23 / 0.04)',
        'focus': '0 0 0 3px rgb(194 97 58 / 0.25)',
      },
      maxWidth: {
        'content': '68ch',
        'wide':    '90rem',
        'page':    '76rem',
      },
      transitionDuration: {
        'fast':   '120ms',
        'normal': '200ms',
        'slow':   '350ms',
      },
      transitionTimingFunction: {
        'default': 'cubic-bezier(0.4, 0, 0.2, 1)',
        'out':     'cubic-bezier(0, 0, 0.2, 1)',
      },
    },
  },
  plugins: [],
}
