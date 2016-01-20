#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>
#include <sstream>
using namespace std;

vector<pair<int, string>> to_vector(map<string, int> mymap) {

	vector<pair<int, string>> result;
	for (map<string, int>::iterator itr = mymap.begin(); itr != mymap.end();
		itr++) {
		result.push_back(pair<int, string>(itr->second, itr->first));
	}
	return result;
}

pair<int, string> split_str(string str) {
	string delimiter = "|";
	
	size_t pos = str.find(delimiter);
	string count = str.substr(0, pos);
	str.erase(0, pos + delimiter.length()); 
	pos = str.find(delimiter);
	// get the substring sourceIP
	string ip = str.substr(0, pos);
	stringstream ss(count);
	int d;
	ss >> d;
	return pair<int, string>(d, ip);
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

void sort_and_flush_to_file(map<string, int> &myMap, int file_count) {
	// put freq map into vector so that we can sort it
	vector<pair<int, string>> freq_vec = to_vector(myMap);
	sort(freq_vec.begin(), freq_vec.end(), mycomp());

	string file_name = "out" + to_string(file_count) + ".txt";
	ofstream outfile(file_name);

	// output format is count|IPaddress
	int i;
	for (i = 0; i < freq_vec.size() - 1; i++) {
		outfile << freq_vec[i].first << "|" << freq_vec[i].second << "\n";
	}
	outfile << freq_vec[i].first << "|" << freq_vec[i].second;
	myMap.clear();
	outfile.close();
}

std::ifstream& goto_line(std::ifstream& file, int num){
    file.seekg(ios::beg);
    for(int i=0; i < num - 1; ++i){
        file.ignore(std::numeric_limits<std::streamsize>::max(),'\n');
    }
    return file;
}

bool fill_bucket(vector<pair<int, string>> &myVec, int &num_lines, ifstream &myfile) {
	int i = myVec.size();
	int counter = 0;
	// seek to num_lines read
	goto_line(myfile, num_lines + 1);
	string str;
	while (getline(myfile, str)) {
		myVec.push_back(split_str(str));
		counter++;
		num_lines++;
		if (counter == i) {
			break;
		}
	}
	getline(myfile, str);
	// already at the end of file
	if (myfile.bad() && counter == 0)
		return false;
	else
		return true;
	myfile.close();
}

void write_partial_result_to_file(vector<pair<int, string>> &myVec, ofstream &myfile) {
	int i;
	for (i = 0; i < myVec.size() - 1; i++) {
		myfile << myVec[i].first << "|" << myVec[i].second << "\n";
	}

	myfile << myVec[i].first << "|" << myVec[i].second << "\n";
}

void read_remaining_file(vector<pair<int, string>> &myVec, int &num_lines, ifstream &myfile,
	ofstream &outfile) {
	int i = myVec.size();
	int counter = 0;
	// seek to num_lines read
	goto_line(myfile, num_lines + 1);
	string str;
	while (getline(myfile, str)) {
		myVec.push_back(split_str(str));
		counter++;
		num_lines++;
		if (counter == i) {
			write_partial_result_to_file(myVec, outfile);
			myVec.clear();
			counter = 0;
		}
	}
}

int main() {

	ifstream myfile("input.txt");
	string str;
	// ASSUMING number of users are small enough
	// that it can fit in map in memory
	// Otherwise you need to distribute it across machines and 
	// maintain distributed hash of count
	map<string, int> freq_map;

	// read really large file line by line
	size_t chunk_size = 24; 
	// decides the size of input log that we are allowed to process
	// after these many reads of input log file, we must flush to disk
	int counter = 0;
	int chunk_count = 0;
	while (getline(myfile, str)) {
		process_str(str, freq_map);
		if (counter++ == chunk_size) {
			++chunk_count;
			sort_and_flush_to_file(freq_map, chunk_count);
			counter = 0;
		}
	}
	++chunk_count;
	sort_and_flush_to_file(freq_map, chunk_count);

	// I have chunk_count number of sorted files - need to do a n-way merge
	vector<int> merge_idx(3, 0);
	vector<vector<pair<int, string>>> merge_buckets(2);

	size_t read_chunk_size = 4;

	for (int i = 0; i < chunk_count; i++) {
		string file_name = "out" + to_string(chunk_count) + ".txt";
		ifstream inFile(file_name);
		for (int j = 0; j < read_chunk_size; j++) {
			string str;
			getline(inFile, str);
			merge_buckets[i].push_back(split_str(str));
		}
		inFile.close();
	}
	ifstream file1("out1.txt");
	ifstream file2("out2.txt");
	// final sorted output
	ofstream out_sorted("sorted.txt", ios_base::app);

	goto_line(file1, read_chunk_size + 1);
	goto_line(file2, read_chunk_size + 1);
	int lines_read1 = read_chunk_size;
	int lines_read2 = read_chunk_size;

	int i = 0, j = 0;
	vector<pair<int, string>> result;
	bool done = false;
	int count_merge = 0;
	while (!done) {
		while (i < merge_buckets[0].size() && j < merge_buckets[1].size()) {
			if (merge_buckets[0][i].first < merge_buckets[1][j].first) {
				result.push_back(merge_buckets[0][i]);
				i++;
			}
			else {
				result.push_back(merge_buckets[1][j]);
				j++;	
			}
			count_merge++;
		}
		write_partial_result_to_file(result, out_sorted);
		result.clear();
		// if found top 5 results, break out of the main loop!
		if (count_merge >= 5) {
			done = true;
			continue;
		}

		// first bucket empty, fill it again
		if (i == merge_buckets[0].size()) {
			bool flag = fill_bucket(merge_buckets[0], lines_read1, file1);
			if (flag == false) {
				// file 1 is empty
				read_remaining_file(merge_buckets[1], lines_read2, file2, out_sorted);
				done = true;
			}
			else {
				i = 0;
			}
		}
		
		// first bucket empty, fill it again
		if (j == merge_buckets[1].size()) {
			bool flag = fill_bucket(merge_buckets[1], lines_read2, file2);	
			if (flag == false) {
				// file 2 is empty
				read_remaining_file(merge_buckets[0], lines_read1, file1, out_sorted);
				done = true;
			}
			else {
				j = 0;
			}
		}		
	}

	file1.close();
	file2.close();
	out_sorted.close();

	cout << "Printing the top 4 users IP address [IP count]" << endl;
	
	ifstream sorted_file("sorted.txt");
	string line;
	int p = 0;
	while (getline(sorted_file, line)) {
		cout << split_str(line).second << endl;
		if (++p == 4) {
			break;
		}
	}

	return 0;
}