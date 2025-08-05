import streamlit as st
import pandas as pd
from datetime import datetime
import uuid
from pymongo import MongoClient

# Sample data - replace this with your actual data source
# def get_sample_data():
#     return [
#         {
#             "transcript": "Hello, my name is John and I'm excited to be here today. I have been working in software development for the past 5 years and I'm passionate about creating innovative solutions that help businesses grow.",
#             "score": 85,
#             "feedback": "Great introduction, clear communication skills demonstrated.",
#             "participant_name": "John Smith",
#             "updated_at": datetime(2024, 1, 15, 10, 30),
#         },
#         {
#             "transcript": "Hi everyone, I'm Sarah. I've been in marketing for 8 years and I love creating campaigns that connect with people on an emotional level. My biggest achievement was launching a campaign that increased brand awareness by 40%.",
#             "score": 92,
#             "feedback": "Excellent presentation, strong examples provided.",
#             "participant_name": "Sarah Johnson",
#             "updated_at": datetime(2024, 1, 15, 11, 45),
#         },
#         {
#             "transcript": "Good morning, I'm Mike. I'm a data analyst with 3 years of experience. I enjoy finding patterns in complex datasets and turning them into actionable insights for decision makers.",
#             "score": 78,
#             "feedback": "Good technical knowledge, could improve on presentation confidence.",
#             "participant_name": "Mike Davis",
#             "updated_at": datetime(2024, 1, 15, 14, 20),
#         }
#     ]

def init_mongodb():
    try:
        # MongoDB connection string - replace with your actual connection string
        mongodb_uri = "mongodb://localhost:27017/"
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')  # Force connection check
        db = client['anganwadi']
        st.success("Connected to MongoDB successfully!")
        return db
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {str(e)}")
        return None
    
db = init_mongodb()

def get_data_from_db():
    global db
    if db is None:
        raise Exception("Database connection not available")
    
    data = list(db.call_tracking.find())
    final_data = []
    for i in data:
        final_data.append({
            "transcript": i["transcript"],
            "score": i["score"],
            "feedback": i["feedback"],
            "participant_name": i["participant_name"],
            "updated_at": i["updated_at"]
        })
    return final_data

def main():
    st.set_page_config(page_title="Participant Data Dashboard", layout="wide")
    
    st.title("📊 Participant Data Dashboard")
    st.markdown("---")
    
    # Get data
    # data = get_sample_data()
    data  = get_data_from_db()
    # Create DataFrame for display (without transcript)
    display_data = []
    for item in data:
        display_data.append({
            "Participant Name": item["participant_name"],
            "Score": item["score"],
            "Feedback": item["feedback"],
            "Updated At": item["updated_at"].strftime("%Y-%m-%d %H:%M"),
        })
    
    df = pd.DataFrame(display_data)
    
    # Display the main table
    st.subheader("📋 Participant Summary")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score",
                help="Participant score out of 100",
                min_value=0,
                max_value=100,
            ),
        }
    )
    
    st.markdown("---")
    
    # Transcript section with expandable content
    st.subheader("📝 Transcripts")
    st.write("Click on a participant below to view their transcript:")
    
    # Create columns for better layout
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.write("**Select Participant:**")
        
        # Create buttons for each participant
        for i, item in enumerate(data):
            if st.button(
                f"{item['participant_name']} (Score: {item['score']})",
                key=f"participant_{i}",
                use_container_width=True
            ):
                st.session_state.selected_participant = i
    
    with col2:
        if 'selected_participant' in st.session_state:
            selected_idx = st.session_state.selected_participant
            selected_data = data[selected_idx]
            
            st.write("**Transcript Details:**")
            
            # Display participant info
            info_col1, info_col2 = st.columns(2)
            with info_col1:
                st.metric("Participant", selected_data['participant_name'])
                st.metric("Score", selected_data['score'])
            
            with info_col2:
                st.metric("Updated", selected_data['updated_at'].strftime("%Y-%m-%d %H:%M"))
            
            # Display feedback
            st.write("**Feedback:**")
            st.info(selected_data['feedback'])
            
            # Display transcript
            st.write("**Full Transcript:**")
            st.text_area(
                "Transcript Content",
                selected_data['transcript'],
                height=150,
                disabled=True,
                label_visibility="collapsed"
            )
        else:
            st.write("👆 Select a participant above to view their transcript")
    
    # Alternative approach using expanders
    st.markdown("---")
    st.subheader("📖 Alternative View - Expandable Transcripts")
    
    for i, item in enumerate(data):
        with st.expander(f"{item['participant_name']} - Score: {item['score']} (Click to expand transcript)"):
            col_a, col_b = st.columns([1, 2])
            
            with col_a:
                st.write("**Details:**")
                st.write(f"**Name:** {item['participant_name']}")
                st.write(f"**Score:** {item['score']}")
                st.write(f"**Updated:** {item['updated_at'].strftime('%Y-%m-%d %H:%M')}")
                st.write(f"**Feedback:** {item['feedback']}")
            
            with col_b:
                st.write("**Transcript:**")
                st.write(item['transcript'])

if __name__ == "__main__":
    main()