# School Scraper - User Guide

## Getting Started

1. **Access the Application**
   - Open your web browser and navigate to the application URL
   - The application will load showing the "Start New Search" screen

## Running a Scrape

### Step 1: Select a State
- On the main screen, you'll see a dropdown menu labeled "Select State"
- Click the dropdown and choose the state you want to scrape
- Available states include: Alabama, Alaska, Arizona, Arkansas, California, Colorado, Connecticut, Delaware, Florida, Georgia, Hawaii, Idaho, Illinois, Indiana, Iowa, Kansas, Kentucky, Louisiana, Maine, Maryland, Massachusetts, Michigan, Minnesota, Mississippi, Missouri, Montana, Nebraska, Nevada, New Hampshire, New Jersey, New Mexico, New York, North Carolina, North Dakota, Ohio, Oklahoma, Oregon, Pennsylvania, Rhode Island, South Carolina, South Dakota, Tennessee, Texas, Utah, Vermont, Virginia, Washington, West Virginia, Wisconsin, Wyoming

### Step 2: Start the Scrape
- Click the "Start Scraping" button
- The application will automatically begin processing schools in that state

## During the Scrape

### Progress View
Once the scrape starts, you'll see:

- **Completed Counties**: Number of counties that have finished processing
- **Processed Schools**: Total number of schools found and processed
- **Current County**: The county currently being processed (with a pulse animation)
- **Activity Log**: Real-time updates showing what's happening
- **Run Statistics**: 
  - Elapsed time since the scrape started
  - Estimated time remaining
  - Current status

### What Happens
The system automatically:
1. Searches for schools in each county
2. Discovers contact pages on school websites
3. Extracts contact information
4. Enriches emails using Hunter.io (if available)
5. Compiles all data into a final CSV file

**Note**: Processing time varies by state. Larger states (like California or Texas) may take several hours.

## After Completion

### Download Results
When the scrape finishes:

1. You'll see a "Scraping Complete" screen with summary statistics
2. Click the **"Download CSV"** button
3. The CSV file will download to your computer's Downloads folder
4. The filename will be: `{State}_leads_YYYYMMDD_HHMMSS.csv`

### Understanding the Results
The CSV file contains:
- School name
- District name
- Contact name
- Contact email
- Contact role/title
- School address
- Additional metadata

## Troubleshooting

### The scrape seems stuck
- Check the "Activity Log" - it updates every 2 seconds
- If no updates appear for 10+ minutes, refresh the page and check the backend status

### No results found
- Some states or counties may have limited school data available
- Try a different state or check the activity log for specific errors

### Download button not working
- Ensure the scrape has fully completed (check the summary screen)
- Try refreshing the page
- Check your browser's download settings

## Tips

- **Start Small**: Test with smaller states (like Delaware) before running large states
- **Monitor Progress**: Keep the browser tab open during processing to see real-time updates
- **Be Patient**: Large states can take 2-4 hours to complete
- **Check Activity Log**: The activity log shows detailed progress for each county

## Support

If you encounter issues:
1. Check the Activity Log for error messages
2. Verify your internet connection is stable
3. Try refreshing the page
4. Contact technical support with:
   - The state you were scraping
   - Screenshot of any error messages
   - Time when the issue occurred

