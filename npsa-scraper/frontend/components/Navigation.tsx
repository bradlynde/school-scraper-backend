import Image from 'next/image';

const Navigation = () => {
  return (
    <nav className="w-full bg-white border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          {/* Logo */}
          <div className="flex items-center">
            <Image
              src="/npsa-logo.png"
              alt="Nonprofit Security Advisors"
              width={200}
              height={60}
              className="h-auto"
              priority
            />
          </div>
          
          {/* Navigation Tabs */}
          <div className="flex items-center space-x-1">
            <a
              href="#"
              className="px-4 py-2 text-[#1e3a5f] font-medium border-b-2 border-[#1e3a5f]"
            >
              Scraper
            </a>
          </div>
        </div>
      </div>
    </nav>
  );
};

export default Navigation;

