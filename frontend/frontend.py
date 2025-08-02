import streamlit as st
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import os
from bson import ObjectId

# Page configuration
st.set_page_config(page_title="Hiring Potter", layout="wide")

Postion_id= 0
Room_id = 0
# MongoDB connection
@st.cache_resource
def init_mongodb():
    try:
        # MongoDB connection string - replace with your actual connection string
        mongodb_uri = st.secrets.get("MONGODB_URI", "mongodb://localhost:27017/")
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')  # Force connection check
        db = client['hiring_potter']
        st.success("Connected to MongoDB successfully!")
        return db
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {str(e)}")
        return None

# Initialize MongoDB
db = init_mongodb()

# MongoDB helper functions
def save_position_to_db(position_title, position_description, questions):
    global Postion_id, Room_id
    """Save position data to MongoDB"""
    if db is None:
        return False, "Database connection not available"
    
    try:
        
        position_data = {
            "position_title": position_title,
            "position_description": position_description,
            "questions": questions,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "position_id": Postion_id,
            "room_id": Room_id,
            "status": "active"  # Default status
        }
        
        result = db.positions.insert_one(position_data)
        Postion_id +=1
        Room_id +=1 
        return True, str(result.inserted_id)
    except Exception as e:
        return False, str(e)

def load_positions_from_db():
    """Load all positions from MongoDB"""
    if db is None:
        return []
    
    try:
        positions = list(db.positions.find().sort("created_at", -1))
        return positions
    except Exception as e:
        st.error(f"Error loading positions: {str(e)}")
        return []

def update_position_in_db(position_id, position_title, position_description, questions):
    """Update existing position in MongoDB"""
    if db is None:
        return False, "Database connection not available"
    
    try:
        update_data = {
            "position_title": position_title,
            "position_description": position_description,
            "questions": questions,
            "updated_at": datetime.now()
        }
        
        result = db.positions.update_one(
            {"_id": ObjectId(position_id)},
            {"$set": update_data}
        )
        return True, "Position updated successfully"
    except Exception as e:
        return False, str(e)

def delete_position_from_db(position_id):
    """Delete position from MongoDB"""
    if db is None:
        return False, "Database connection not available"
    
    try:
        result = db.positions.delete_one({"_id": ObjectId(position_id)})
        return True, "Position deleted successfully"
    except Exception as e:
        return False, str(e)

# Initialize session state variables
if 'questions' not in st.session_state:
    st.session_state.questions = []

if 'position_description' not in st.session_state:
    st.session_state.position_description = ""

if 'position_title' not in st.session_state:
    st.session_state.position_title = ""

if 'current_position_id' not in st.session_state:
    st.session_state.current_position_id = None

if 'calls_data' not in st.session_state:
    st.session_state.calls_data = []

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select Page", ["Position & Questions", "Saved Positions", "Call Tracking"])

# Page 1: Position Description and Questions
if page == "Position & Questions":
    st.title("📋 Position Description & Interview Questions")
    st.markdown("---")
    
    # Position Title Section
    st.header("Position Details")
    position_title = st.text_input(
        "Position Title:",
        value=st.session_state.position_title,
        placeholder="Enter position title (e.g., Senior Software Engineer)"
    )
    st.session_state.position_title = position_title
    
    # Position Description Section
    st.subheader("Position Description")
    st.write("Enter the position description (maximum 200 words):")
    
    position_desc = st.text_area(
        "Position Description",
        value=st.session_state.position_description,
        height=400,
        max_chars=1200,  # Approximate 200 words
        placeholder="Enter detailed position description here..."
    )
    
    # Word count
    word_count = len(position_desc.split())
    if word_count > 200:
        st.error(f"⚠️ Word count: {word_count}/200 - Please reduce the description")
    else:
        st.info(f"Word count: {word_count}/200")
    
    # Update session state
    st.session_state.position_description = position_desc
    
    st.markdown("---")
    
    # Questions Section
    st.header("Interview Questions")
    st.write(f"Add interview questions (maximum 10 questions) - Current: {len(st.session_state.questions)}/10")
    
    # Add new question
    col1, col2 = st.columns([4, 1])
    
    with col1:
        new_question = st.text_input("Enter a new question:", placeholder="Type your question here...")
    
    with col2:
        if st.button("➕ Add Question", disabled=len(st.session_state.questions) >= 10):
            if new_question.strip():
                if len(st.session_state.questions) < 10:
                    st.session_state.questions.append(new_question.strip())
                    st.success("Question added!")
                    st.rerun()
                else:
                    st.error("Maximum 10 questions allowed!")
            else:
                st.error("Please enter a question!")
    
    # Display existing questions
    if st.session_state.questions:
        st.subheader("Current Questions:")
        for i, question in enumerate(st.session_state.questions, 1):
            col1, col2 = st.columns([10, 1])
            with col1:
                st.write(f"**{i}.** {question}")
            with col2:
                if st.button("🗑️", key=f"delete_{i}", help="Delete this question"):
                    st.session_state.questions.pop(i-1)
                    st.rerun()
    else:
        st.info("No questions added yet. Use the form above to add questions.")
    
    # Action buttons
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Save to Database
        if st.button("💾 Save to Database", type="primary"):
            if position_title.strip() and position_desc.strip() and word_count <= 200:
                if st.session_state.current_position_id:
                    # Update existing position
                    success, message = update_position_in_db(
                        st.session_state.current_position_id,
                        position_title.strip(),
                        position_desc.strip(),
                        st.session_state.questions
                    )
                else:
                    # Create new position
                    success, message = save_position_to_db(
                        position_title.strip(),
                        position_desc.strip(),
                        st.session_state.questions
                    )
                    if success:
                        st.session_state.current_position_id = message
                
                if success:
                    st.success("Position saved to database successfully!")
                else:
                    st.error(f"Error saving position: {message}")
            else:
                st.error("Please fill in position title, description (within word limit), and add at least one question.")
    
    with col2:
        # Clear form
        if st.button("🗑️ Clear Form", type="secondary"):
            st.session_state.questions = []
            st.session_state.position_description = ""
            st.session_state.position_title = ""
            st.session_state.current_position_id = None
            st.rerun()
    
    with col3:
        # New Position
        if st.button("📝 New Position", type="secondary"):
            st.session_state.questions = []
            st.session_state.position_description = ""
            st.session_state.position_title = ""
            st.session_state.current_position_id = None
            st.rerun()

# Page 2: Saved Positions
elif page == "Saved Positions":
    st.title("📚 Saved Positions")
    st.markdown("---")
    
    # Load positions from database
    positions = load_positions_from_db()
    
    if positions:
        st.header(f"Total Saved Positions: {len(positions)}")
        
        for position in positions:
            with st.expander(f"📋 {position['position_title']} - Created: {position['created_at'].strftime('%Y-%m-%d %H:%M')}"):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.subheader("Position Description:")
                    st.write(position['position_description'])
                    
                    st.subheader("Interview Questions:")
                    if position['questions']:
                        for i, question in enumerate(position['questions'], 1):
                            st.write(f"**{i}.** {question}")
                    else:
                        st.write("No questions added for this position.")
                
                with col2:
                    st.write("**Actions:**")
                    
                    # Load position for editing
                    if st.button("✏️ Edit", key=f"edit_{position['_id']}"):
                        st.session_state.position_title = position['position_title']
                        st.session_state.position_description = position['position_description']
                        st.session_state.questions = position['questions']
                        st.session_state.current_position_id = str(position['_id'])
                        st.success("Position loaded for editing! Go to 'Position & Questions' page.")
                    
                    # Delete position
                    if st.button("🗑️ Delete", key=f"delete_{position['_id']}", type="secondary"):
                        success, message = delete_position_from_db(str(position['_id']))
                        if success:
                            st.success("Position deleted successfully!")
                            st.rerun()
                        else:
                            st.error(f"Error deleting position: {message}")
    else:
        st.info("No saved positions found. Create a position in the 'Position & Questions' page and save it to the database.")

# Page 3: Call Tracking
elif page == "Call Tracking":
    st.title("📞 Call Tracking")
    st.markdown("---")
    
    # Add new call section
    st.header("Add New Call")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        caller_name = st.text_input("Caller Name:", placeholder="Enter caller's name")
    
    with col2:
        caller_number = st.text_input("Caller Number:", placeholder="Enter phone number")
    
    with col3:
        caller_email = st.text_input("Caller Email:", placeholder="Enter email address")
    
    if st.button("📞 Add Call Record"):
        if caller_name.strip() and caller_number.strip() and caller_email.strip():
            new_call = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Caller Name": caller_name.strip(),
                "Caller Number": caller_number.strip(),
                "Caller Email": caller_email.strip()
            }
            st.session_state.calls_data.append(new_call)
            st.success("Call record added successfully!")
            st.rerun()
        else:
            st.error("Please fill in all fields!")
    
    st.markdown("---")
    
    # Display calls table
    st.header("Call Records")
    
    if st.session_state.calls_data:
        # Create DataFrame
        df = pd.DataFrame(st.session_state.calls_data)
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Calls", len(df))
        with col2:
            st.metric("Today's Calls", len(df[df['Timestamp'].str.contains(datetime.now().strftime("%Y-%m-%d"))]))
        with col3:
            st.metric("Unique Callers", df['Caller Name'].nunique())
        
        # Display table
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )
        
        # Download option
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Call Records as CSV",
            data=csv,
            file_name=f"call_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
        # Clear all records
        if st.button("🗑️ Clear All Records", type="secondary"):
            st.session_state.calls_data = []
            st.rerun()
    
    else:
        st.info("No call records found. Add a new call record using the form above.")

# Footer
st.markdown("---")
st.markdown("*Built with Streamlit & MongoDB* 🚀")

# MongoDB Setup Instructions (in sidebar)
if db is None:
    st.sidebar.markdown("---")
    st.sidebar.error("⚠️ MongoDB Connection Failed")
    st.sidebar.markdown("""
    **Setup Instructions:**
    1. Install pymongo: `pip install pymongo`
    2. Set up MongoDB connection in secrets.toml:
    ```
    MONGODB_URI = "mongodb://localhost:27017/"
    ```
    Or use MongoDB Atlas cloud connection string.
    """)
else:
    st.sidebar.markdown("---")
    st.sidebar.success("✅ MongoDB Connected")