package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var formats = map[string]string{
	"combined":          "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent \"$http_referer\" \"$http_user_agent\"",
	"combined_duration": "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent \"$http_referer\" \"$http_user_agent\".* $duration_seconds",
}

var patterns = map[string]string{
	"$remote_addr":      "(?P<remote_addr>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3})",
	"$remote_user":      "[[:alnum:]-]+",
	"$time_local":       "[[:alnum:] :/+-]+",
	"$request":          "(?P<verb>[A-Z]+) (?P<url>[^\\?]+)(\\?.*)? HTTP/[0-9.]+",
	"$status":           "(?P<status>[0-9]+)",
	"$body_bytes_sent":  "(?P<bytes>[0-9]+)",
	"$http_referer":     "[^\"]+",
	"$http_user_agent":  "[^\"]+",
	"$duration_seconds": "(?P<duration>[0-9.]+)",
}

var cfg struct {
	NumInTop         int
	RouteMergeFactor int
}

func main() {
	flag.IntVar(&cfg.NumInTop, "t", 10, "Number of items in top")
	flag.IntVar(&cfg.RouteMergeFactor, "m", 10, "Merge factor for route computation")
	flag.Parse()
	file := flag.Arg(0)
	if file == "" {
		fmt.Println("Usage: loganalyzer logfile")
		return
	}
	parseFile(file, "")
}

func buildRe(format string) *regexp.Regexp {
	format = strings.Replace(format, "[", "\\[", -1)
	format = strings.Replace(format, "]", "\\]", -1)
	for k, p := range patterns {
		format = strings.Replace(format, k, p, -1)
	}
	return regexp.MustCompile(format)
}

func guessFormatRe(format, logline string) *regexp.Regexp {
	//5.135.212.100 - - [21/Aug/2014:00:10:14 +0200] "GET /categorie/blouses-femme/nantes HTTP/1.1" 200 22054 "-" "kysoebot" 221 0.497
	if format == "" {
		bestLen := 0
		bestName := ""
		for name, fmt := range formats {
			if len(fmt) > bestLen {
				re := buildRe(fmt)
				if re.MatchString(logline) {
					bestLen = len(fmt)
					bestName = name
					format = fmt
				}
			}
		}
		if format != "" {
			fmt.Printf("Guessed format : %s\n", bestName)
		} else {
			fmt.Println("Unable to guess log format - please specify")
			os.Exit(1)
		}
	}
	return buildRe(format)
}

func getPossibleSubRoutes(parts []string) []string {
	if len(parts) == 1 {
		return []string{"/" + parts[0], "/xxx"}
	}
	sub := getPossibleSubRoutes(parts[1:])
	routes := []string{}
	for _, r := range sub {
		routes = append(routes, "/"+parts[0]+r, "/xxx"+r)
	}
	return routes
}

func getPossibleRoutes(url string) []string {
	parts := strings.Split(url, "/")
	if len(parts) == 2 { // /xxx
		return []string{url}
	}
	sub := getPossibleSubRoutes(parts[2:])
	routes := []string{}
	for _, r := range sub {
		routes = append(routes, "/"+parts[1]+r)
	}
	return routes
}

type stats struct {
	route                    string
	subIsMerged              bool
	subRoutes                map[string]*stats
	call, error5xx, error404 int
	duration                 float32
}

func (s *stats) addRequest(url, status string, duration float32) {
	s.call++
	switch status {
	case "404":
		s.error404++
	case "500":
		s.error5xx++
	case "502":
		s.error5xx++
	case "503":
		s.error5xx++
	case "504":
		s.error5xx++
	}
	s.duration += duration
	parts := strings.Split(url, "/")
	s.route = parts[0]
	if len(parts) >= 2 && parts[1] != "" { // /xxx

		// try to guess if next path part is a slug or a page number
		if strings.Count(parts[1], "-") >= 3 {
			s.subIsMerged = true
		}
		if _, err := strconv.Atoi(parts[1]); err == nil {
			s.subIsMerged = true
		}

		if s.subRoutes == nil {
			s.subRoutes = map[string]*stats{}
		}
		var subUrl string
		if len(parts) > 2 {
			subUrl = "/" + strings.Join(parts[2:], "/")
		}

		if _, found := s.subRoutes["xxx"]; !found {
			s.subRoutes["xxx"] = &stats{}
		}
		s.subRoutes["xxx"].addRequest("xxx"+subUrl, status, duration)

		if s.subIsMerged == false {
			if _, found := s.subRoutes[parts[1]]; !found {
				s.subRoutes[parts[1]] = &stats{}
			}
			s.subRoutes[parts[1]].addRequest(parts[1]+subUrl, status, duration)
		}
	}
}

func (s *stats) flatten() statsList {
	if s.subRoutes == nil || len(s.subRoutes) == 0 {
		return []*stats{s}
	}
	flat := []*stats{}
	if len(s.subRoutes) > s.call/cfg.RouteMergeFactor {
		flat = append(flat, s.subRoutes["xxx"].flatten()...)
	} else {
		for k, sub := range s.subRoutes {
			if k != "xxx" || len(s.subRoutes) == 1 {
				flat = append(flat, sub.flatten()...)
			}
		}
	}
	for _, f := range flat {
		f.route = s.route + "/" + f.route
	}
	return flat
}

type statsList []*stats

type lessFn func(a, b *stats) bool

type sortable struct {
	fn    lessFn
	stats statsList
}

func (s sortable) Len() int {
	return len(s.stats)
}

func (s sortable) Less(i, j int) bool {
	return s.fn(s.stats[i], s.stats[j])
}

func (s sortable) Swap(i, j int) {
	s.stats[i], s.stats[j] = s.stats[j], s.stats[i]
}

func (s statsList) sortByCall() {
	sortable := sortable{
		fn: func(a, b *stats) bool {
			return b.call < a.call
		},
		stats: s,
	}
	sort.Sort(sortable)
}

func (s statsList) sortBy404() {
	sortable := sortable{
		fn: func(a, b *stats) bool {
			return b.error404 < a.error404
		},
		stats: s,
	}
	sort.Sort(sortable)
}

func (s statsList) sortBy5xx() {
	sortable := sortable{
		fn: func(a, b *stats) bool {
			return b.error5xx < a.error5xx
		},
		stats: s,
	}
	sort.Sort(sortable)
}

func (s statsList) sortByDuration() {
	sortable := sortable{
		fn: func(a, b *stats) bool {
			return b.duration/float32(b.call) < a.duration/float32(a.call)
		},
		stats: s,
	}
	sort.Sort(sortable)
}

func (s statsList) sortByCost() {
	sortable := sortable{
		fn: func(a, b *stats) bool {
			return b.duration < a.duration
		},
		stats: s,
	}
	sort.Sort(sortable)
}

func parseFile(file string, format string) {
	var f io.ReadCloser
	f, err := os.Open(file)
	if err != nil {
		fmt.Println("error opening file= ", err)
		os.Exit(1)
	}
	defer f.Close()
	if strings.HasSuffix(file, ".gz") {
		f, err = gzip.NewReader(f)
		if err != nil {
			fmt.Println("error decompressing file= ", err)
			os.Exit(1)
		}
	}

	var re *regexp.Regexp
	urlIndex := -1
	statusIndex := -1
	durationIndex := -1
	statsTree := &stats{}
	errors := 0
	r := bufio.NewReader(f)
	for {
		s, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if re == nil {
			re = guessFormatRe(format, s)
			for i, name := range re.SubexpNames() {
				switch name {
				case "url":
					urlIndex = i
				case "status":
					statusIndex = i
				case "duration":
					durationIndex = i
				}
			}
		}
		match := re.FindStringSubmatch(s)
		if match == nil {
			errors++
			continue
		}
		url := match[urlIndex]

		status := match[statusIndex]
		var duration float64
		if durationIndex >= 0 {
			durationStr := match[durationIndex]
			duration, _ = strconv.ParseFloat(durationStr, 32)
		}

		statsTree.addRequest(url, status, float32(duration))
	}
	allstats := statsTree.flatten()
	// Top calls
	fmt.Println("** TOP CALLS **")
	allstats.sortByCall()
	for i := 0; i < len(allstats) && i < cfg.NumInTop; i++ {
		fmt.Printf("%d. %s : %d\n", i, allstats[i].route, allstats[i].call)
	}
	// Top calls
	fmt.Println("** TOP 404 **")
	allstats.sortBy404()
	for i := 0; i < len(allstats) && i < cfg.NumInTop; i++ {
		fmt.Printf("%d. %s : %d\n", i, allstats[i].route, allstats[i].error404)
	}
	// Top calls
	fmt.Println("** TOP 5xx **")
	allstats.sortBy5xx()
	for i := 0; i < len(allstats) && i < cfg.NumInTop; i++ {
		fmt.Printf("%d. %s : %d\n", i, allstats[i].route, allstats[i].error5xx)
	}
	// Top calls
	fmt.Println("** TOP AVG DURATION **")
	allstats.sortByDuration()
	for i := 0; i < len(allstats) && i < cfg.NumInTop; i++ {
		fmt.Printf("%d. %s : %.3fs\n", i, allstats[i].route, allstats[i].duration/float32(allstats[i].call))
	}
	// Top calls
	fmt.Println("** TOP COST **")
	allstats.sortByCost()
	for i := 0; i < len(allstats) && i < cfg.NumInTop; i++ {
		fmt.Printf("%d. %s : %.3fs\n", i, allstats[i].route, allstats[i].duration)
	}
}
