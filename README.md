# 2390 Songs-a-Played
Songs played more than 10 times between Dec 17 and Dec 23, 2017 on Toronto's CHFI All-Christmas radio. Ornament decorative wedges show breakdown by artist. Hover for more details.

Data obtained through CHFI's JSON API, indexed into Elasticsearch, queried, and d3-ed. Visualization inspiration from The Pudding's The Largest Vocabulary in Hip Hop.

## Getting Data
Get the last 1000 songs
http://newplayer.rogersradio.ca/CHFI/widget/recently_played?num_per_page=1000

## Querying
```
POST /_search
{
	"size": 0,
	"aggs": { 
		"song": { 
			"terms" : { 
				"field" : "song_title",
				"size": 80
			},
			"aggs" : {
                "num_artists" : { "cardinality" : { "field" : "artist" } },
                "top_artists" : { "terms" : { "field" : "artist" } }
            }
		} 
	}
}
```