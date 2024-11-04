import pandas as pd
import streamlit as st
import psycopg2
import pandas
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import time
from geopy.geocoders import Nominatim
import plotly.express as px
import seaborn as sns
import matplotlib.dates as mdates
from functools import lru_cache
from streamlit_autorefresh import st_autorefresh


def fetch_voting_stats():
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgresql2024",
        host="localhost", port="5432")
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute("""
        SELECT count(*) total_count FROM tweets_iphone14
    """)
    total_count = cur.fetchone()[0]

    return total_count

def fetch_data(query):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgresql2024",
        host="localhost", port="5432")
    cur = conn.cursor()
    df = pandas.read_sql(query, conn)
    conn.close()
    return df

query = "SELECT * FROM tweets_iphone14"
data = fetch_data(query)

def pie_chart(data):
    df = pd.DataFrame(data)
    polarity_counts = df['sentiment'].value_counts()
    fig_pie, ax_pie = plt.subplots(figsize=(6,4))
    fig_pie.patch.set_facecolor('black')
    patches, texts, autotexts = ax_pie.pie(polarity_counts, labels=polarity_counts.index,
                                           autopct='%1.1f%%', startangle=90,colors=['#845ec2', '#d6effa', '#2189a7'])
    for text in texts:
        text.set_color('white')
    centre_circle = plt.Circle((0, 0), 0.70, fc='black')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    ax_pie.axis('equal')
    plt.tight_layout()
    ax_pie.set_title("Sentiment Distribution", color="white")
    return fig_pie




def word_cloud(data):
    df = pd.DataFrame(data)
    all_text = ' '.join(df['processed_text'])
    custom_stopwords = set(STOPWORDS).union({'apple', 'iphone', 'pro', 'max'})
    wordcloud = WordCloud(width=500, height=300, stopwords=custom_stopwords,
                          background_color='black').generate(all_text)
    fig_wc, ax_wc = plt.subplots()
    ax_wc.imshow(wordcloud, interpolation='bilinear')
    ax_wc.axis('off')
    return fig_wc

def top5_leaderboard_fig(df):
    #df = pd.DataFrame(data)
    leaderboard = df[['username', 'user_description', 'user_location', 'tweet_text',
                      'tweet_like_count', 'sentiment']].sort_values(by='tweet_like_count', ascending=False)[:5]
    return leaderboard

def tweet_activity_fig(data):
    df = pd.DataFrame(data)
    df['date_time'] = pd.to_datetime(df['date_time'])
    tweets_by_day = df.groupby(df['date_time'].dt.month).size().reset_index(name='tweet_count')
    plt.figure()
    plt.plot(tweets_by_day['date_time'], tweets_by_day['tweet_count'])
    return plt


def tweet_activity_fig2(df):
    #df = pd.DataFrame(data)
    df['date_time'] = pd.to_datetime(df['date_time'])
    tweets_by_day = df.groupby(df['date_time'].dt.date).size().reset_index(name='tweet_count')
    #sns.set(style="whitegrid", palette="muted")

    # Create the plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=tweets_by_day, x='date_time', y='tweet_count', marker='o', color='teal')

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
    #plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.gcf().autofmt_xdate()  # Rotate date labels for readability

    #plt.figure(figsize=(10, 6))
    #plt.plot_date(tweets_by_day['date_time'], tweets_by_day['tweet_count'], linestyle='solid')
    plt.xlabel("Date")
    plt.ylabel("Tweets")
    #plt.title("Daily Tweet Activity")
    #plt.tight_layout()
    return plt

def geo_board(data):
    df = pd.DataFrame(data)
    geolocator = Nominatim(user_agent="geoapiExercises")

    # Function to get coordinates
    def get_coordinates(location):
        try:
            loc = geolocator.geocode(location)
            if loc:
                return loc.latitude, loc.longitude
        except:
            return None, None
        return None, None

    # Apply the function to get latitude and longitude
    df['coordinates'] = df['user_location'].apply(lambda x: get_coordinates(x))
    df[['latitude', 'longitude']] = pd.DataFrame(df['coordinates'].tolist(), index=df.index)
    df = df.dropna(subset=['latitude', 'longitude'])  # Drop rows with missing coordinates

    # Plotting with Plotly
    fig_geo = px.scatter_geo(df,
                         lat='latitude',
                         lon='longitude',
                         hover_name='user_location',
                         size='tweet_count',
                         projection="natural earth",
                         title="Tweet Counts by User Location")
    fig_geo.update_geos(showcoastlines=True, coastlinecolor="Black", showland=True, landcolor="LightGreen")
    return fig_geo


def geo_board2(df):
    #df = pd.DataFrame(data)
    geolocator = Nominatim(user_agent="geoapiExercises")

    def get_coordinates(location):
        try:
            loc = geolocator.geocode(location, timeout=10)
            if loc:
                return loc.latitude, loc.longitude
        except:
            return None, None
        return None, None

    # Отримання координат для кожного розташування
    df['coordinates'] = df['user_location'].apply(lambda x: get_coordinates(x) if pd.notnull(x) else (None, None))
    df[['latitude', 'longitude']] = pd.DataFrame(df['coordinates'].tolist(), index=df.index)
    df = df.dropna(subset=['latitude', 'longitude'])  # Видаляємо рядки без координат

    # Побудова графіка з Plotly
    fig_geo = px.scatter_geo(df,
                             lat='latitude',
                             lon='longitude',
                             hover_name='user_location',
                             projection="natural earth",
                             title="Tweet Counts by User Location")
    fig_geo.update_geos(showcoastlines=True, coastlinecolor="Black", showland=True, landcolor="LightGreen")
    return fig_geo

def update_data():
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    total_count = fetch_voting_stats()

    left_column, middle_column, right_column = st.columns(3)
    df = pd.DataFrame(data)
    df['tweet_like_count'] = pd.to_numeric(df['tweet_like_count'])
    avg_likes = round(df['tweet_like_count'].mean(), 1)
    top_user = df.loc[df['tweet_like_count'].idxmax()]['username']
    with left_column:
        st.subheader("Tweets Processed:")
        st.subheader(f"{total_count}")
    with middle_column:
        st.subheader("Average Tweet Likes:")
        st.subheader(f"{avg_likes}")
    with right_column:
        st.subheader("Top User:")
        st.subheader(f"{top_user}")


    # Display total voters and candidates metrics
    st.markdown("""---""")
    #col1 = st.columns(1)
    #col1.metric("Total Voters", total_count)
    #st.write("Total voters:", total_count)

    # leaderboard of top 5 users with coolest tweets (by likes)
    pie_column, wordcloud_column = st.columns(2)
    with pie_column:
        pie_fig = pie_chart(data)
        st.pyplot(pie_fig)
    with wordcloud_column:
        word_cloud_fig = word_cloud(data)
        st.pyplot(word_cloud_fig)

    lead_column = st.columns(1)
    with lead_column[0]:
        st.subheader("Top users with the most popular tweets")
        leaderboard = top5_leaderboard_fig(df)
        st.dataframe(leaderboard)
    activity_col = st.columns(1)
    with activity_col[0]:
        st.markdown("### Tweet Activity Over Time")
        activity_fig = tweet_activity_fig2(df)
        st.pyplot(activity_fig)

    #activity_fig = tweet_activity_fig(data)
    #st.pyplot(activity_fig)
    #fig_geo = geo_board(data)
    #st.pyplot(fig_geo)
    # Update the last refresh time
    st.session_state['last_update'] = time.time()


# Sidebar layout
def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    #refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)


    # Button to manually refresh data
    if st.sidebar.button('Refresh Data'):
        update_data()



# Title of the Streamlit dashboard
#st.markdown("<h1 style='display:inline; margin:0;'>Twitter Real-Time Analytics Dashboard</h1>", unsafe_allow_html=True)
#st.title('Twitter Real-Time Analytics Dashboard')

st.set_page_config(layout="wide")
st.title(":bar_chart: Twitter Real-Time Analytics Dashboard")
st.markdown(
    """
    <style>
    .dataframe {
        width: 1600px;  /* Set your desired width here */
    }
    </style>
    """,
    unsafe_allow_html=True
)

update_data()
st_autorefresh(interval=20 * 1000, key="auto")