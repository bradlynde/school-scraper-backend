"use client";

import { useState } from "react";
import Image from "next/image";

type SidebarProps = {
  activeTab: 'school' | 'church';
  onTabChange: (tab: 'school' | 'church') => void;
  isCollapsed?: boolean;
  onCollapseChange?: (collapsed: boolean) => void;
};

const Sidebar = ({ activeTab, onTabChange, isCollapsed: externalCollapsed, onCollapseChange }: SidebarProps) => {
  const [internalCollapsed, setInternalCollapsed] = useState(false);
  const isCollapsed = externalCollapsed !== undefined ? externalCollapsed : internalCollapsed;
  
  const handleCollapse = () => {
    const newState = !isCollapsed;
    if (externalCollapsed === undefined) {
      setInternalCollapsed(newState);
    }
    if (onCollapseChange) {
      onCollapseChange(newState);
    }
  };

  return (
    <aside
      className={`fixed left-0 top-0 h-full bg-white border-r border-gray-200 shadow-sm transition-all duration-300 z-50 ${
        isCollapsed ? 'w-20' : 'w-64'
      }`}
    >
      <div className="flex flex-col h-full">
        {/* Logo Section */}
        <div className={`p-6 border-b border-gray-200 flex items-center ${isCollapsed ? 'justify-center' : 'justify-between'}`}>
          <Image
            src="/npsa-logo.png"
            alt="Nonprofit Security Advisors"
            width={160}
            height={48}
            className="h-auto"
            priority
          />
          {!isCollapsed && (
            <button
              onClick={handleCollapse}
              className="p-2 rounded-lg hover:bg-gray-100 transition-colors ml-auto"
              aria-label="Collapse sidebar"
            >
              <svg
                className="w-5 h-5 text-gray-600 transition-transform"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M15 19l-7-7 7-7"
                />
              </svg>
            </button>
          )}
          {isCollapsed && (
            <button
              onClick={handleCollapse}
              className="absolute top-6 right-2 p-2 rounded-lg hover:bg-gray-100 transition-colors"
              aria-label="Expand sidebar"
            >
              <svg
                className="w-5 h-5 text-gray-600 transition-transform rotate-180"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M15 19l-7-7 7-7"
                />
              </svg>
            </button>
          )}
        </div>

        {/* Navigation Items */}
        <nav className="flex-1 p-4 space-y-2">
          <button
            onClick={() => onTabChange('school')}
            className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
              activeTab === 'school'
                ? 'bg-[#1e3a5f] text-white shadow-md'
                : 'text-gray-700 hover:bg-gray-100'
            }`}
            title={isCollapsed ? "School Scraper" : undefined}
          >
            <svg
              className="w-5 h-5 flex-shrink-0"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"
              />
            </svg>
            {!isCollapsed && <span>School Scraper</span>}
          </button>

          <button
            onClick={() => onTabChange('church')}
            className={`w-full flex items-center gap-4 px-4 py-3 rounded-lg font-medium transition-all duration-200 ${
              activeTab === 'church'
                ? 'bg-[#1e3a5f] text-white shadow-md'
                : 'text-gray-700 hover:bg-gray-100'
            }`}
            title={isCollapsed ? "Church Scraper" : undefined}
          >
            <svg
              className="w-5 h-5 flex-shrink-0"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"
              />
            </svg>
            {!isCollapsed && <span>Church Scraper</span>}
          </button>
        </nav>
      </div>
    </aside>
  );
};

export default Sidebar;

