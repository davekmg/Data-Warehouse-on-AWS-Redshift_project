import configparser
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


def visualize_songplays_by_day(conn, year, month):
	"""
	plots the songplays of the month on a line graph
	"""

	# fetch songplays into a dataframe
	df = fetch_songplays(conn, year, month)
	#print(df)

	x_values = df['day']
	y_values = df['no_of_songplays']
	fig, ax = plt.subplots()
	
	# set chart title and label axes.
	ax.set_title("Song Plays Analysis", fontsize=24)
	ax.set_xlabel("Day of the Month", fontsize=14)
	ax.set_ylabel("Song Plays", fontsize=14)
	ax.plot(x_values, y_values, linewidth=3)
	plt.savefig('songplays_analysis.png')


def fetch_songplays(conn, year, month):
	"""
	fetches a summary of the songplays for the specified month
	"""

	sql_query = pd.read_sql_query("SELECT count(sp.songplay_id) no_of_songplays, \
									t.year, t.month, t.day \
									FROM songplays sp \
									JOIN artists ar ON sp.artist_id = ar.artist_id \
									JOIN time t ON sp.start_time = t.start_time \
									WHERE t.year = "+year+" AND t.month = "+month+" \
									GROUP BY t.year, t.month, t.day \
									ORDER BY t.day;", conn)
	df = pd.DataFrame(sql_query, columns=['no_of_songplays','day'])
	return df   


def main():
	config = configparser.ConfigParser()
	config.read('dwh.cfg')

	conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
	cur = conn.cursor()

	try:
		year = '2018' 
		month = '11'
		visualize_songplays_by_day(conn, year, month)
	except Exception as e:
		print(e)
		conn.close()
	finally:
		conn.close()


if __name__ == "__main__":
	main()