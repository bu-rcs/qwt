# import faicons as fa
# import plotly.express as px
# import pandas as pd

# # Load data and compute static values
# from shared import app_dir, dataset
# from shinywidgets import render_plotly

# from shiny import reactive, render, App
# from shiny import ui as sui
# from shiny.express import input, ui
# import plotly.graph_objects as go
# from pathlib import Path

# import gpu_job

# # Adjust the column to get the min and max waiting time
# bill_rng = (dataset.first_job_waiting_time.min(), dataset.first_job_waiting_time.max())
# # Ensure the 'year' column is of integer type
# dataset['year'] = dataset['year'].astype(int)

# month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
# dataset['month'] = pd.Categorical(dataset['month'], categories=month_order, ordered=True)

# # Create the main UI layout with both the navigation bar and year selection toggled by one button
# sui.page_fluid(
#     # Navigation Bar that will be toggled
#     sui.tags.div(
#         sui.navset_bar(
#             sui.nav_panel("All Jobs"),
#             sui.nav_panel("GPU Job"),
#             sui.nav_panel("MPI Job"),
#             id="selected_navset_bar",
#             title="Job Type",
#         ),
#         id="nav-bar-content",  # Assign an ID for targeting via JavaScript
#         style="background-color: #f8f9fa; padding: 10px;"
#     ),
    
#     # Page title and sidebar with toggle button
#     sui.tags.div(
#         # Toggle Button to show/hide both the navigation bar and the year selection
#         sui.input_action_button(
#             "toggle_button",
#             "Toggle Job Types and Year Selection",
#             class_="btn btn-primary",
#             style="margin-bottom: 10px;"
#         ),
        
#         # Year Selection Bar that will be toggled
#         sui.tags.div(
#             sui.input_checkbox_group(
#                 "years",
#                 "Select Year(s)",
#                 list(range(2013, 2025)),  # Year range from 2013 to 2024
#                 selected=[2024],  # Default to selecting the year 2024
#                 inline=True  # Arrange checkboxes horizontally
#             ),
#             id="top-bar-content",  # Assign an ID for targeting via JavaScript
#             class_="top-bar",
#             style="background-color: #f8f9fa; padding: 10px; display: flex; align-items: center; justify-content: space-between;"
#         ),
#     ),
    
#     # JavaScript to handle toggling of both the navigation bar and year selection bar
#     sui.tags.script(
#         """
#         // Maintain a toggle state variable to track the visibility state
#         var isHidden = false;

#         document.getElementById("toggle_button").addEventListener("click", function() {
#             var topBarContent = document.getElementById("top-bar-content");
#             var navBarContent = document.getElementById("nav-bar-content");
            
#             if (isHidden) {
#                 // If hidden, show both elements
#                 topBarContent.style.display = "flex";
#                 navBarContent.style.display = "block";
#             } else {
#                 // If shown, hide both elements
#                 topBarContent.style.display = "none";
#                 navBarContent.style.display = "none";
#             }

#             // Toggle the state
#             isHidden = !isHidden;
#         });
#         """
#     )
# )

# with ui.sidebar(open="desktop"):
#     ui.input_slider(
#         "first_job_waiting_time",
#         "Waiting Time",
#         min=bill_rng[0],
#         max=bill_rng[1],
#         value=bill_rng,
#         pre="$",
#     ),

#     ui.input_checkbox_group(
#         "job_type",
#         "Job Type",
#         list(dataset.job_type.unique()),
#         selected=list(dataset.job_type.unique()),
#         inline=True,
#     )
#     ui.input_action_button("select_all", "Select All")
#     ui.input_action_button("unselect_all", "Unselect All")


# # Add main content
# ICONS = {
#     "min": fa.icon_svg("arrow-down"),
#     "max": fa.icon_svg("arrow-up"),
#     "mean": fa.icon_svg("users"),
#     "median": fa.icon_svg("battery-half"),
#     "currency-dollar": fa.icon_svg("dollar-sign"),
#     "ellipsis": fa.icon_svg("ellipsis"),
#     "clock": fa.icon_svg("clock"),
#     "chart-bar": fa.icon_svg("chart-bar"),
#     "calendar": fa.icon_svg("calendar"),
#     "comment": fa.icon_svg("comment"),
#     "bell": fa.icon_svg("bell"),
#     "camera": fa.icon_svg("camera"),
#     "heart": fa.icon_svg("heart"),
# }

# with ui.layout_columns(fill=False):
#     with ui.value_box(showcase=ICONS["min"]):
#         "Min Waiting Time"

#         @render.express
#         def min_waiting_time():
#             d = dataset_data()
#             if d.shape[0] > 0:
#                 min_waiting_time = d.first_job_waiting_time.min() / 60
#                 if min_waiting_time > 60:
#                     f"{min_waiting_time / 60:.2f} hour"
#                 else:
#                     f"{min_waiting_time:.2f} min"

#     with ui.value_box(showcase=ICONS["max"]):
#         "Max Waiting Time"

#         @render.express
#         def max_waiting_time():
#             d = dataset_data()
#             if d.shape[0] > 0:
#                 max_waiting_time = d.first_job_waiting_time.max() / 60
#                 if max_waiting_time > 60:
#                     f"{max_waiting_time / 60:.2f} hour"
#                 else:
#                     f"{max_waiting_time:.2f} min"

#     with ui.value_box(showcase=ICONS["mean"]):
#         "Mean Waiting Time"

#         @render.express
#         def mean_waiting_time():
#             d = dataset_data()
#             if d.shape[0] > 0:
#                 mean_waiting_time = d.first_job_waiting_time.mean() / 60
#                 if mean_waiting_time > 60:
#                     f"{mean_waiting_time / 60:.2f} hour"
#                 else:
#                     f"{mean_waiting_time:.2f} min"

#     with ui.value_box(showcase=ICONS["median"]):
#         "Median Waiting Time"

#         @render.express
#         def median_waiting_time():
#             d = dataset_data()
#             if d.shape[0] > 0:
#                 med_waiting_time = d.first_job_waiting_time.median() / 60
#                 if med_waiting_time > 60:
#                     f"{med_waiting_time / 60:.2f} hour"
#                 else:
#                     f"{med_waiting_time:.2f} min"



# with ui.layout_columns(col_widths=[6, 6, 6, 6, 6]):
#     with ui.card(full_screen=True):
#         ui.card_header("Dataset Data")

#         @render.data_frame
#         def table():
#             df = dataset_data()
#             df_modified = df.copy()
#             df_modified['first_job_waiting_time'] = (df_modified['first_job_waiting_time'] / 60).round(2)  # Convert waiting time from seconds to minutes
#             df_modified.rename(columns={'first_job_waiting_time': 'first_job_waiting_time (min)'}, inplace=True)            
#             return render.DataGrid(df_modified)

#     with ui.card(full_screen=True):
#         with ui.card_header(class_="d-flex justify-content-between align-items-center"):
#             "Waiting Time vs Job Type"
#             with ui.popover(title="Add a color variable", placement="top"):
#                 ICONS["ellipsis"]
#                 ui.input_radio_buttons(
#                     "scatter_color",
#                     None,
#                     ["job_type", "none"],
#                     inline=True,
#                 )

#         @render_plotly
#         def scatterplot():
#             color = input.scatter_color()
#             data = dataset_data()
#             filtered_data = data[data["first_job_waiting_time"] > 0]
#             # Group the filtered data by 'job_type' and sum the 'first_job_waiting_time'
#             grouped_data = filtered_data.groupby("job_type")["first_job_waiting_time"].sum().reset_index()
#             fig = px.scatter(
#                 grouped_data,
#                 x="first_job_waiting_time",
#                 y="job_type",
#                 color=None if color == "none" else color,
#                 trendline="lowess",
#             )
#             return fig

#     # with ui.card(full_screen=True):
#     #     with ui.card_header(class_="d-flex justify-content-between align-items-center"):
#     #         "Waiting Time Percentages"

#     #     @render_plotly
#     #     def wait_perc():
#     #         dat = dataset_data()
#     #         dat = dat[dat["first_job_waiting_time"] > 0]  # Ignore rows where first_job_waiting_time is zero
#     #         total_waiting_time = dat["first_job_waiting_time"].sum()
#     #         dat["percent"] = dat["first_job_waiting_time"] / total_waiting_time * 100
#     #         pie_data = dat.groupby("job_type")["percent"].sum().reset_index()

#     #         fig = px.pie(pie_data, names="job_type", values="percent", title="Waiting Time Percentages by Job Type")
#     #         fig.update_traces(textposition='inside', textinfo='percent+label')

#     #         return fig

        
#     with ui.card(full_screen=True):
#         with ui.card_header(class_="d-flex justify-content-between align-items-center"):
#             "Box Plot of Job Waiting Time by Month & Year"

#         @render_plotly
#         def job_waiting_time_by_month():
#             data = dataset_data()
#             # Filter data for job types and years
#             data = data[data['job_type'].isin(input.job_type())]
#             data = data[data['year'].isin(map(int, input.years()))]
#             # Convert waiting time from seconds to hours
#             data['job_waiting_time (hours)'] = data['first_job_waiting_time'] / 3600
#             # Create box plot
#             fig = px.box(
#                 data,
#                 x='month',
#                 y='job_waiting_time (hours)',
#                 color='year',
#                 facet_col='job_type',
#                 title="Job Waiting Time by Month and Year (Box Plot in Hours)"
#             )
#             # Update layout for better visualization
#             fig.update_layout(
#                 yaxis=dict(range=[0, 20]),  # Adjust Y-axis to focus on lower range
#                 boxmode='group',  # Group boxes for better comparison
#                 title=None,  # Remove the main title
#                 showlegend=True  # Show legend for better understanding
#             )

#             # Remove x-axis title for all subplots
#             for axis in fig.layout:
#                 if axis.startswith('xaxis'):
#                     fig.layout[axis].title.text = None

#             # Update traces to include jittered points with a distinct color
#             fig.update_traces(
#                 marker=dict(
#                     size=6,  # Adjust marker size
#                     opacity=0.7,  # Adjust marker opacity
#                     color='blue',  # Set point color to a contrasting color (e.g., black)
#                     line=dict(width=1, color='white')  # Add a white outline for further contrast
#                 ),
#                 boxpoints='all',  # Show all points
#                 jitter=0.3,  # Add jitter for better visibility
#                 pointpos=0  # Position the points within the box
#             )
            
#             # Ensure consistent month order
#             fig.update_xaxes(categoryorder='array', categoryarray=month_order)

#             return fig
    
#     with ui.card(full_screen=True):
#         with ui.card_header(class_="d-flex justify-content-between align-items-center2"):
#             "3D Bubble Chart of Average Job Waiting Time by Year & Job Type"

#         @render_plotly
#         def job_waiting_time_3d():
#             data = dataset_data()
#             # Filter data for job types and years
#             data = data[data['job_type'].isin(input.job_type())]
#             data = data[data['year'].isin(map(int, input.years()))]
#             # Aggregate waiting time by month, year, and job_type
#             data = data.groupby(['year', 'month', 'job_type'])['first_job_waiting_time'].mean().reset_index()
#             # Convert waiting time from seconds to hours
#             data['first_job_waiting_time (hours)'] = data['first_job_waiting_time'] / 3600
#             # Check for and handle NaN values
#             data['first_job_waiting_time (hours)'].fillna(0, inplace=True)  # Replace NaNs with 0
#             # Ensure 'month' is categorical and ordered correctly
#             month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
#             data['month'] = pd.Categorical(data['month'], categories=month_order, ordered=True)
#             # Create 3D scatter plot
#             fig = px.scatter_3d(
#                 data, 
#                 x='month', 
#                 y='job_type', 
#                 z='first_job_waiting_time (hours)', 
#                 size='first_job_waiting_time (hours)', 
#                 color='month', 
#                 hover_data=['year', 'job_type'],
#                 title="3D Bubble Chart of Job Waiting Time by Month & Job Type"
#             )
#             # Update layout to ensure all months are displayed
#             fig.update_layout(
#                 scene=dict(
#                     xaxis=dict(
#                         tickmode='array',
#                         tickvals=list(range(12)),
#                         ticktext=month_order
#                     )
#                 ),
#                 scene_zaxis_type="linear"  # Set z-axis to linear scale
#             )
#             return fig

# ui.include_css(app_dir / "styles.css")

# # --------------------------------------------------------
# # Reactive calculations and effects
# # --------------------------------------------------------

# @reactive.calc
# def dataset_data():
#     bill = input.first_job_waiting_time()
#     years = input.years()
    
#     # Convert selected years to integers
#     years = list(map(int, years))
    
#     idx1 = dataset.first_job_waiting_time.between(bill[0], bill[1])
#     idx2 = dataset.job_type.isin(input.job_type())
#     idx3 = dataset.year.isin(years) if years else True  # Check for selected years if any

#     return dataset[idx1 & idx2 & idx3]


# @reactive.effect
# @reactive.event(input.select_all)
# def _():
#     ui.update_checkbox_group("job_type", selected=list(dataset.job_type.unique()))

# @reactive.effect
# @reactive.event(input.unselect_all)
# def _():
#     ui.update_checkbox_group("job_type", selected=[])






# import faicons as fa
# import plotly.graph_objects as go
# import pandas as pd

# # Load data and compute static values
# from shared import app_dir, dataset
# from shinywidgets import output_widget, render_plotly
# from shiny import App, reactive, render, ui
# # be careful when use shiny.express, it's conflicting with some shiny core templates
# import plotly.express as px

# # Adjust the column to get the min and max waiting time
# bill_rng = (dataset.first_job_waiting_time.min(), dataset.first_job_waiting_time.max())
# # Ensure the 'year' column is of integer type
# dataset['year'] = dataset['year'].astype(int)

# month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
# dataset['month'] = pd.Categorical(dataset['month'], categories=month_order, ordered=True)



# ICONS = {
#     "min": fa.icon_svg("arrow-down"),
#     "max": fa.icon_svg("arrow-up"),
#     "mean": fa.icon_svg("users"),
#     "median": fa.icon_svg("battery-half"),
#     "currency-dollar": fa.icon_svg("dollar-sign"),
#     "ellipsis": fa.icon_svg("ellipsis"),
#     "clock": fa.icon_svg("clock"),
#     "chart-bar": fa.icon_svg("chart-bar"),
#     "calendar": fa.icon_svg("calendar"),
#     "comment": fa.icon_svg("comment"),
#     "bell": fa.icon_svg("bell"),
#     "camera": fa.icon_svg("camera"),
#     "heart": fa.icon_svg("heart"),
# }

# # Add page title and sidebar
# app_ui = ui.page_fluid(
#     # Top bar container (Navigation Bar + Year Selection Bar)
#     ui.tags.div(
#         # Navigation Bar that will be toggled
#         ui.tags.div(
#             ui.navset_bar(
#                 ui.nav_panel("All Jobs"),
#                 ui.nav_panel("GPU Job"),
#                 ui.nav_panel("MPI Job"),
#                 id="selected_navset_bar",
#                 title="Job Type",
#             ),
#             id="nav-bar-content",  # Assign an ID for targeting via JavaScript
#             style="background-color: #f8f9fa; padding: 10px;"
#         ),
#         # Toggle Button and Year Selection Bar that will be toggled
#         ui.tags.div(
#             # Toggle Button to show/hide both the navigation bar and the year selection
#             ui.input_action_button(
#                 "toggle_button",
#                 "Toggle Job Types and Year Selection",
#                 class_="btn btn-primary",
#                 style="margin-bottom: 10px;",
#             ),
            
#             # Year Selection Bar
#             ui.tags.div(
#                 ui.input_checkbox_group(
#                     "years",
#                     "Select Year(s)",
#                     list(range(2013, 2025)),  # Year range from 2013 to 2024
#                     selected=[2024],  # Default to selecting the year 2024
#                     inline=True  # Arrange checkboxes horizontally
#                 ),
#                 id="top-bar-content",  # Assign an ID for targeting via JavaScript
#                 class_="top-bar",
#                 style="background-color: #f8f9fa; padding: 10px; display: flex; align-items: center; justify-content: space-between;"
#             ),
#         ),
#         style="margin-bottom: 20px;"  # Add spacing between top bar and the rest of the content
#     ),
    
#     # Existing Sidebar and Main Content Layout
#     ui.page_sidebar(
#         ui.sidebar(
#             ui.input_slider(
#                 "first_job_waiting_time",
#                 "Waiting Time",
#                 min=bill_rng[0],
#                 max=bill_rng[1],
#                 value=bill_rng,
#                 pre="$",
#             ),
#             ui.input_checkbox_group(
#                 "job_type",
#                 "Job Type",
#                 list(dataset.job_type.unique()),
#                 selected=list(dataset.job_type.unique()),
#                 inline=True,
#             ),
#             ui.input_action_button("select_all", "Select All"),
#             ui.input_action_button("unselect_all", "Unselect All"),
#             open="desktop",
#         ),
        
#         ui.layout_columns(
#             ui.value_box(
#                 "Min Waiting Time", ui.output_text("min_waiting_time"), showcase=ICONS["min"]
#             ),
#             ui.value_box(
#                 "Max Waiting Time", ui.output_text("max_waiting_time"), showcase=ICONS["max"]
#             ),
#             ui.value_box(
#                 "Mean Waiting Time", ui.output_text("mean_waiting_time"), showcase=ICONS["mean"]
#             ),
#             ui.value_box(
#                 "Median Waiting Time", ui.output_text("median_waiting_time"), showcase=ICONS["median"]
#             ),
#             fill=False,
#         ),
       
#         ui.layout_columns(
#             ui.card(
#                 ui.card_header("Dataset Data"),
#                 ui.output_data_frame("table"),  # Display the table rendered by the render function
#                 full_screen=True
#             ),
#             ui.card(
#                 ui.card_header(
#                     "Waiting Time vs Job Type",
#                     ui.popover(
#                         ICONS["ellipsis"],
#                         ui.input_radio_buttons(
#                             "scatter_color",
#                             None,
#                             ["job_type", "none"],
#                             inline=True,
#                         ),
#                         title="Add a color variable",
#                         placement="top",
#                     ),
#                     class_="d-flex justify-content-between align-items-center",
#                 ),
#                 output_widget("scatterplot"),  # Display the scatterplot rendered by the render function
#                 full_screen=True
#             ),
#             ui.card(
#                 ui.card_header(
#                     "Box Plot of Job Waiting Time by Month & Year",
#                     class_="d-flex justify-content-between align-items-center"
#                 ),
#                 output_widget("job_waiting_time_by_month"),  # Display the box plot rendered by the render function
#                 full_screen=True
#             ),
#             ui.card(
#                 ui.card_header(
#                     "3D Bubble Chart of Average Job Waiting Time by Year & Job Type",
#                     class_="d-flex justify-content-between align-items-center"
#                 ),
#                 output_widget("job_waiting_time_3d"),  # Display the 3D bubble chart rendered by the render function
#                 full_screen=True
#             ),
#             col_widths=[6, 6, 6, 6]
#         ),
#         ui.include_css(app_dir / "styles.css"),
#         title="Queue Waiting Time",
#         fillable=True,
#     ),
    
#     # JavaScript to handle toggling of both the navigation bar and year selection bar
#     ui.tags.script(
#         """
#         // Maintain a toggle state variable to track the visibility state
#         var isHidden = false;

#         document.getElementById("toggle_button").addEventListener("click", function() {
#             var topBarContent = document.getElementById("top-bar-content");
#             var navBarContent = document.getElementById("nav-bar-content");
            
#             if (isHidden) {
#                 // If hidden, show both elements
#                 topBarContent.style.display = "flex";
#                 navBarContent.style.display = "block";
#             } else {
#                 // If shown, hide both elements
#                 topBarContent.style.display = "none";
#                 navBarContent.style.display = "none";
#             }

#             // Toggle the state
#             isHidden = !isHidden;
#         });
#         """
#     )
# )


# def server(input, output, session):
#     @reactive.calc
#     def dataset_data():
#         bill = input.first_job_waiting_time()
#         years = input.years()
        
#         # Convert selected years to integers
#         years = list(map(int, years))
        
#         idx1 = dataset.first_job_waiting_time.between(bill[0], bill[1])
#         idx2 = dataset.job_type.isin(input.job_type())
#         idx3 = dataset.year.isin(years) if years else True  # Check for selected years if any

#         return dataset[idx1 & idx2 & idx3]

#     # Define the rendering logic for the waiting times
#     @render.text
#     def min_waiting_time():
#         d = dataset_data()  # Replace with your data fetching logic
#         print(d.shape[0])
#         if d.shape[0] > 0:
#             min_waiting_time = d.first_job_waiting_time.min() / 60
#             if min_waiting_time > 60:
#                 return f"{min_waiting_time / 60:.2f} hour"
#             else:
#                 return f"{min_waiting_time:.2f} min"
        

#     @render.text
#     def max_waiting_time():
#         d = dataset_data()  # Replace with your data fetching logic
#         if d.shape[0] > 0:
#             max_waiting_time = d.first_job_waiting_time.max() / 60
#             if max_waiting_time > 60:
#                 return f"{max_waiting_time / 60:.2f} hour"
#             else:
#                 return f"{max_waiting_time:.2f} min"

#     @render.text
#     def mean_waiting_time():
#         d = dataset_data()  # Replace with your data fetching logic
#         if d.shape[0] > 0:
#             mean_waiting_time = d.first_job_waiting_time.mean() / 60
#             if mean_waiting_time > 60:
#                 return f"{mean_waiting_time / 60:.2f} hour"
#             else:
#                 return f"{mean_waiting_time:.2f} min"

#     @render.text
#     def median_waiting_time():
#         d = dataset_data()  # Replace with your data fetching logic
#         if d.shape[0] > 0:
#             med_waiting_time = d.first_job_waiting_time.median() / 60
#             if med_waiting_time > 60:
#                 return f"{med_waiting_time / 60:.2f} hour"
#             else:
#                 return f"{med_waiting_time:.2f} min"

#     @render.data_frame
#     def table():
#         df = dataset_data()  # Replace with your data fetching logic
#         df_modified = df.copy()
#         df_modified['first_job_waiting_time'] = (df_modified['first_job_waiting_time'] / 60).round(2)  # Convert waiting time from seconds to minutes
#         df_modified.rename(columns={'first_job_waiting_time': 'first_job_waiting_time (min)'}, inplace=True)            
#         return render.DataGrid(df_modified)

#     @render_plotly
#     def scatterplot():
#         color = input.scatter_color()
#         data = dataset_data()
#         filtered_data = data[data["first_job_waiting_time"] > 0]
#         # Group the filtered data by 'job_type' and sum the 'first_job_waiting_time'
#         grouped_data = filtered_data.groupby("job_type")["first_job_waiting_time"].sum().reset_index()
#         fig = px.scatter(
#             grouped_data,
#             x="first_job_waiting_time",
#             y="job_type",
#             color=None if color == "none" else color,
#             trendline="lowess",
#         )
#         return fig

#     @render_plotly
#     def job_waiting_time_by_month():
#         data = dataset_data()
#         # Filter data for job types and years
#         data = data[data['job_type'].isin(input.job_type())]
#         data = data[data['year'].isin(map(int, input.years()))]
#         # Convert waiting time from seconds to hours
#         data['job_waiting_time (hours)'] = data['first_job_waiting_time'] / 3600
#         # Create box plot
#         fig = px.box(
#             data,
#             x='month',
#             y='job_waiting_time (hours)',
#             color='year',
#             facet_col='job_type',
#             title="Job Waiting Time by Month and Year (Box Plot in Hours)"
#         )
#         # Update layout for better visualization
#         fig.update_layout(
#             yaxis=dict(range=[0, 20]),  # Adjust Y-axis to focus on lower range
#             boxmode='group',  # Group boxes for better comparison
#             title=None,  # Remove the main title
#             showlegend=True  # Show legend for better understanding
#         )

#         # Remove x-axis title for all subplots
#         for axis in fig.layout:
#             if axis.startswith('xaxis'):
#                 fig.layout[axis].title.text = None

#         # Update traces to include jittered points with a distinct color
#         fig.update_traces(
#             marker=dict(
#                 size=6,  # Adjust marker size
#                 opacity=0.7,  # Adjust marker opacity
#                 color='blue',  # Set point color to a contrasting color (e.g., black)
#                 line=dict(width=1, color='white')  # Add a white outline for further contrast
#             ),
#             boxpoints='all',  # Show all points
#             jitter=0.3,  # Add jitter for better visibility
#             pointpos=0  # Position the points within the box
#         )
        
#         # Ensure consistent month order
#         fig.update_xaxes(categoryorder='array', categoryarray=month_order)

#         return fig

#     # Define the rendering logic for the 3D bubble chart
#     @render_plotly
#     def job_waiting_time_3d():
#         data = dataset_data()
#         # Filter data for job types and years
#         data = data[data['job_type'].isin(input.job_type())]
#         data = data[data['year'].isin(map(int, input.years()))]
#         # Aggregate waiting time by month, year, and job_type
#         data = data.groupby(['year', 'month', 'job_type'])['first_job_waiting_time'].mean().reset_index()
#         # Convert waiting time from seconds to hours
#         data['first_job_waiting_time (hours)'] = data['first_job_waiting_time'] / 3600
#         # Check for and handle NaN values
#         data['first_job_waiting_time (hours)'].fillna(0, inplace=True)  # Replace NaNs with 0
#         # Ensure 'month' is categorical and ordered correctly
#         month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
#         data['month'] = pd.Categorical(data['month'], categories=month_order, ordered=True)
#         # Create 3D scatter plot
#         fig = px.scatter_3d(
#             data, 
#             x='month', 
#             y='job_type', 
#             z='first_job_waiting_time (hours)', 
#             size='first_job_waiting_time (hours)', 
#             color='month', 
#             hover_data=['year', 'job_type'],
#             title="3D Bubble Chart of Job Waiting Time by Month & Job Type"
#         )
#         # Update layout to ensure all months are displayed
#         fig.update_layout(
#             scene=dict(
#                 xaxis=dict(
#                     tickmode='array',
#                     tickvals=list(range(12)),
#                     ticktext=month_order
#                 )
#             ),
#             scene_zaxis_type="linear"  # Set z-axis to linear scale
#         )
#         return fig

#     @reactive.effect
#     @reactive.event(input.select_all)
#     def _():
#         ui.update_checkbox_group("job_type", selected=list(dataset.job_type.unique()))

#     @reactive.effect
#     @reactive.event(input.unselect_all)
#     def _():
#         ui.update_checkbox_group("job_type", selected=[])


# app = App(app_ui, server)


from shiny import App, reactive, render, ui
from homepage import homepage_ui, homepage_server
from gpu_job import gpu_job_ui, gpu_job_server

# Import additional page logic if needed (e.g., GPU Job, MPI Job)
# from gpu_job import gpu_job_ui, gpu_job_server

app_ui = ui.page_fluid(
    ui.tags.div(
        ui.navset_bar(
            ui.nav_panel("All Jobs"),
            ui.nav_panel("GPU Job"),
            ui.nav_panel("MPI Job"),
            id="selected_navset_bar",
            title="Job Type",
        ),
        id="nav-bar-content",
        style="background-color: #f8f9fa; padding: 10px; height: 75px;"
    ),
    ui.output_ui("page_content")
)

def server(input, output, session):
    current_page = reactive.Value("All Jobs")

    @reactive.effect
    def update_page():
        selected_page = input.selected_navset_bar()
        current_page.set(selected_page)

    # Dynamically render UI based on the current page
    @output
    @render.ui
    def page_content():
        if current_page.get() == "All Jobs":
            return homepage_ui()
        elif current_page.get() == "GPU Job":
            return gpu_job_ui()
        elif current_page.get() == "MPI Job":
            return ui.page_fluid("MPI Job Page (UI to be implemented)")

    # Dynamically call server logic based on the current page
    @reactive.effect
    def call_server():
        if current_page.get() == "All Jobs":
            homepage_server(input, output, session)
        elif current_page.get() == "GPU Job":
            gpu_job_server(input, output, session)
        # Add logic here if you implement separate server logic for "MPI Job"
        # elif current_page.get() == "MPI Job":
        #     mpi_job_server(input, output, session)

app = App(app_ui, server)
