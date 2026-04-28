This repo is a side project of mine inspired by some journalism around restaurants closing much 
earlier than before covid. Some examples include https://www.nytimes.com/2022/09/17/nyregion/new-york-city-closing-time.html 

Most of these articles profiled certain restaurants and introduced the trend. I had a thesis that
adjacency to affordable housing was one big factor that influenced whether a neighborhood was able
to bounce back to being a late night spot. The mechanism for this being availability of workers to
work at these establishments late at night.

However, there was not really any quantitative data about this trend. I wanted to be able to explore
this data in a map to make neighborhood level observations on this trend. I made this tool to help 
visualize closing times for New York City restaurants, cafes, and bars. The way this tool works is:

1. Gather restaurants in an area by querying Yelp API
2. Check current hours for those restaurants 
3. Query wayback machine API for the same restaurant's pages except prior to March 2020 to get
precovid hours.
4. Compute the difference and gather data into a CSV
5. Use the CSV to create a map page using OpenStreetMaps to display each restaurant as a dot and the
color of the dot is how much earlier or later the place is open

In order to make this script work, you'll have to get a yelp API key and set your environment var
'yelp_key' to the API key before running the python script. Once it has run, you should see the 
map.html file generated where you can explore the data. 

I've included an example map html file with a subset of data for exploration in case you don't want 
to deal with yelp API keys.
