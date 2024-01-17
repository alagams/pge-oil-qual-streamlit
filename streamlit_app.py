import streamlit as st
import pandas as pd
import plotly.express as px
import sys

T1_COLUMNS = ['Date', 'Btu Content', 'Secific Gravity', 'N2 Mole%', 'C02 Mole%']
T2_COLUMNS = ['Date', 'Btu Content', 'Secific Gravity', 'N2 Mole%', 'C02 Mole%', 
              'Methane Mole%', 'Ethane Mole%']

COLUMN_LIMITS = {
    'Btu Content' : [850,1150],
    'Secific Gravity' : [.45, .97]
}

def plotColByDay(df, x, yColList, options):
    df = df[df['Btu Area'].isin(options)]
    for col in yColList:
        if col == x:
            pass
        else:
            fig = px.box(df.iloc[::-1], x=x, y=col)
            if col in COLUMN_LIMITS.keys():
                fig.update_yaxes(range = COLUMN_LIMITS[col])
            st.plotly_chart(fig)

if __name__ == "__main__":
    
    path = str(sys.argv[1])
    df = pd.read_csv(path)
    areas = df['Btu Area'].unique()
    options = st.multiselect('What Btu Areas?',
                             areas)
    
    df
    plotColByDay(df, "Date", T1_COLUMNS, options)
