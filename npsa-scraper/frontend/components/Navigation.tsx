const Navigation = () => {
  return (
    <nav className="w-full bg-white border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          {/* Logo */}
          <div className="flex items-center">
            <div className="flex flex-col">
              <div className="flex items-center">
                {/* Green dots above 'o' */}
                <div className="flex flex-col items-center mr-1">
                  <div className="flex space-x-1 mb-1">
                    <div className="w-1.5 h-1.5 bg-[#6b8e23] rounded-full"></div>
                    <div className="w-1.5 h-1.5 bg-[#6b8e23] rounded-full"></div>
                    <div className="w-1.5 h-1.5 bg-[#6b8e23] rounded-full"></div>
                    <div className="w-1.5 h-1.5 bg-[#6b8e23] rounded-full"></div>
                    <div className="w-1.5 h-1.5 bg-[#6b8e23] rounded-full"></div>
                  </div>
                  <span className="text-[#1e3a5f] font-semibold text-lg">nonprofit</span>
                </div>
              </div>
              <span className="text-[#6b8e23] font-normal text-base -mt-1">security advisors</span>
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

