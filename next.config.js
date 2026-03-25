/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  output: "standalone",
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    domains: [],
  },
  generateBuildId: async () => {
    return `build-${Date.now()}`
  },
}

module.exports = nextConfig
