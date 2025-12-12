import Image from 'next/image';

type NavigationProps = {
  activeTab: 'school' | 'church';
  onTabChange: (tab: 'school' | 'church') => void;
};

const Navigation = ({ activeTab, onTabChange }: NavigationProps) => {
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
            <button
              onClick={() => onTabChange('school')}
              className={`px-4 py-2 font-medium transition-colors ${
                activeTab === 'school'
                  ? 'text-[#1e3a5f] border-b-2 border-[#1e3a5f]'
                  : 'text-gray-600 hover:text-[#1e3a5f]'
              }`}
            >
              School Scraper
            </button>
            <button
              onClick={() => onTabChange('church')}
              className={`px-4 py-2 font-medium transition-colors ${
                activeTab === 'church'
                  ? 'text-[#1e3a5f] border-b-2 border-[#1e3a5f]'
                  : 'text-gray-600 hover:text-[#1e3a5f]'
              }`}
            >
              Church Scraper
            </button>
          </div>
        </div>
      </div>
    </nav>
  );
};

export default Navigation;

