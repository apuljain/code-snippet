#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>
using namespace std;

vector<pair<int, string>> to_vector(map<string, int> mymap) {

	vector<pair<int, string>> result;
	for (map<string, int>::iterator itr = mymap.begin(); itr != mymap.end();
		itr++) {
		result.push_back(pair<int, string>(itr->second, itr->first));
	}
	return result;
}

void process_str(string str, map<string, int> &freq_count) {
	//format: ts|sourceIP|resourcename|httpstatuscode
	string delimiter = "|";
	
	size_t pos = str.find(delimiter);
	str.erase(0, pos + delimiter.length()); // erase ts
	// string is now sourceIP|resourcename|httpstatuscode
	// again find the location of delim
	pos = str.find(delimiter);
	// get the substring sourceIP
	string user = str.substr(0, pos);

	if (freq_count.find(user) == freq_count.end()) {
		freq_count[user] = 1;
	}
	else {
		freq_count[user]++;
	}
}
struct mycomp {
	bool operator()(const pair<int, string> a, const pair<int, string> b) {
		return a.first > b.first;
	}
};

int main() {

	ifstream myfile("input.txt");
	string str;
	// ASSUMING number of users are small enough
	// that it can fit in map in memory
	// Otherwise you need to distribute it across machines and 
	// maintain distributed hash of count
	map<string, int> freq_map;

	// read really large file line by line
	while (getline(myfile, str)) {
		process_str(str, freq_map);
	}
	// put freq map into vector so that we can sort it
	vector<pair<int, string>> freq = to_vector(freq_map);
	sort(freq.begin(), freq.end(), mycomp());

	int i = 0;
	
	cout << "Printing the top 5 users IP address [IP count]" << endl;
	
	while (i != 5) {
		cout << freq[i].second << " -- " << freq[i].first << endl;
		i++;
	}
	myfile.close();
	return 0;
}