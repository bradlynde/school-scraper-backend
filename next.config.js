/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Allow images from any domain if needed
  images: {
    domains: [],
  },
  // Force fresh build
  generateBuildId: async () => {
    return `build-${Date.now()}`
  },
}

module.exports = nextConfig

