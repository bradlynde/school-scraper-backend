const Navigation = () => {
  return (
    <nav className="w-full bg-white border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          {/* Logo with curved dots above 'o' */}
          <div className="flex items-center">
            <div className="flex flex-col relative">
              {/* Curved dots above 'o' - positioned to arc over the 'o' */}
              <div className="relative mb-1" style={{ height: '8px' }}>
                <svg width="60" height="8" viewBox="0 0 60 8" className="absolute" style={{ left: '8px', top: '0' }}>
                  <circle cx="5" cy="6" r="1.5" fill="#6b8e23" />
                  <circle cx="15" cy="4" r="1.5" fill="#6b8e23" />
                  <circle cx="25" cy="2" r="1.5" fill="#6b8e23" />
                  <circle cx="35" cy="4" r="1.5" fill="#6b8e23" />
                  <circle cx="45" cy="6" r="1.5" fill="#6b8e23" />
                </svg>
              </div>
              {/* nonprofit text */}
              <span className="text-[#1e3a5f] font-semibold text-lg leading-tight">nonprofit</span>
              {/* security advisors text */}
              <span className="text-[#6b8e23] font-normal text-base leading-tight -mt-1">security advisors</span>
            </div>
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

