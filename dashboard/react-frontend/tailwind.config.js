/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        navy:  { 900: '#0f172a', 800: '#1e293b', 700: '#334155' },
        brand: { 500: '#3b82f6', 400: '#60a5fa', 600: '#2563eb' },
      }
    }
  },
  plugins: [],
}
