/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./scraper/web/templates/**/*.html",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
