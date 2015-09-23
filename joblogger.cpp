/*
 * joblogger.cpp
 *
 */


#include "joblogger.h"

ull GetBaseTime(string line)
{
	unsigned ti = 0;
	while((ti<line.length())&&(line[ti] != ':'))
		ti++;
	if ((ti<line.length())&&(line.compare(ti+1,4,"2014") == 0))
	{
		ti += 6; // reach to month
		ull month = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3; // reach to day
		ull day = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull hour = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull minute = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull second = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull milisec = (line[ti]-'0')*100 + (line[ti+1]-'0')*10 + (line[ti+2]-'0');
		ull mstime = milisec + 1000 * (second + 60 * (minute + 60 * (hour + 24 * ((day-1) + 30 * (month-1)))));
		return mstime;
	}
	else
		return -1;
}
// hadoop-npi_e-jobtracker-wdscnl094.log.2014-04-14:2014-04-14 00:54:20,824 ...
// INFO org.apache.hadoop.mapred.JobInProgress: ...
// Input size for job job_201404012233_57242 = 3100. Number of splits = 4

int main ( int arc, char **argv )
{//StatCol filename
	/////////////////////////Just4debug

	string logpath =  argv[1]; //"Debug/jobs_submissions_logs.out";
	ifstream logfile (&logpath[0]); // command line program gets log filename

	string plotpath = logpath + ".dat"; // plot file name
	ofstream plotfile (&plotpath[0]);

	/////////////////////////Link Analysis
	if (plotfile.is_open())
	{
		if (logfile.is_open())
		{
			string line;
			if(!getline (logfile,line))
				return 1; // no line inside the log!!!
			ull BaseTime = GetBaseTime(line);
			do
			{// Now log is ready to analyze!
				unsigned ti = 0;//timestamp index
				while((ti<line.length())&&(line[ti] != ':'))
					ti++;
				if ((ti<line.length())&&(line.compare(ti+1,4,"2014") == 0))
				{
					ti += 6; // reach to month
					ull month = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3; // reach to day
					ull day = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull hour = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull minute = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull second = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull milisec = (line[ti]-'0')*100 + (line[ti+1]-'0')*10 + (line[ti+2]-'0');
					ull difftime = milisec + 1000 * (second + 60 * (minute + 60 * (hour + 24 * ((day-1) + 30 * (month-1)))));

					std::size_t foundjob = line.find("org.apache.hadoop.mapred.JobInProgress: Input size for job",ti);
					if (foundjob != std::string::npos)
					{
						difftime -= BaseTime; // This is crazy!
						BaseTime += difftime; // Save the diff time in difftime and basetime in BaseTime!!!
						if(difftime > 1000000)
							difftime %= 1000000; // difftime greater than 1e6 is odd ! a failure or odd event happened!
						plotfile << difftime << " ";
						cout << difftime << " ";
						foundjob += 59; // to reach to job name
						std::size_t foundeq = line.find("=", foundjob);
						plotfile << line.substr(foundjob, foundeq - foundjob);
						cout << line.substr(foundjob, foundeq - foundjob);

						foundeq += 2; //to reach to size
						unsigned si = foundeq;
						ull size = 0;
						while (( ((int)(line[si])) >= (int)'0' ) && ( ((int)(line[si])) <= (int)'9' ))    // between 0 - 9
						{
							size *= 10;
							size += (line[si]-'0');
							si++;
						}
						plotfile << size << " ";
						cout << size << " ";

						foundeq = line.find("=", si);
						foundeq += 2; // reach to splitsize
						si = foundeq;
						ull splitsize = 0;
						while (( ((int)(line[si])) >= (int)'0' ) && ( ((int)(line[si])) <= (int)'9' ))    // between 0 - 9
						{
							splitsize *= 10;
							splitsize += (line[si]-'0');
							si++;
						}
						plotfile << splitsize << "\n";
						cout << splitsize << "\n";
					}
				}
			}
			while (getline (logfile,line));
			logfile.close();
		}
		plotfile.close();
	}
	return 0; // Indicates that everything went well.
}


