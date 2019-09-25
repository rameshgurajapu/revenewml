def get_top_features(ix, X, shap_values, correlations):
    """
    Function to extract top Shap features.
    """
    import pandas as pd
    import numpy as np

    # Row index must be passed as list
    ix = [ix]

    # Get feature values for that row
    feature_values = X.iloc[ix, :]

    # Get Shap values for that row
    shap_values_ix = pd.DataFrame(shap_values[ix, :], columns=X.columns)

    # Combine SHAP values and feature values
    df = feature_values.append(shap_values_ix)

    # Set row names
    df.index = ['feature_value', 'shap_value']

    # Transpose
    df = df.T

    # Add column with absolute value of Shap
    df['shap_absolute_value'] = abs(df['shap_value'])

    # Add column with direction of Shap
    df['shap_direction'] = np.sign(df['shap_value'])

    # Sort data frame by highest absolute Shap value
    df.sort_values(by='shap_absolute_value', ascending=False, inplace=True)

    # Create column of feature names using index
    df.reset_index(inplace=True)

    # Rename index column
    df.rename(columns={'index': 'feature_name'}, inplace=True)

    # Create container list and set top values for feature and direction
    top_features = [df['feature_name'].iloc[0]]
    shap_direction = [df['shap_direction'].iloc[0]]

    # Iterate through sorted features to find first set of 5 where all correlations < 0.9
    # while len(top_features) < 5:
    for feature, direction in zip(df['feature_name'], df['shap_direction']):
        corr_max = correlations.loc[top_features, feature].values.max()
        # print(f'\nIndex is {i}\nFeature is {feature}\nDirection is {direction}\nMax correlation is {corr_max}\n')
        if corr_max < 0.9:
            top_features.append(feature)
            shap_direction.append(direction)
            break
    
    # Output data frame
    try: 
        return pd.DataFrame({
            'Top1FeatureDesc': top_features[0],
            'Top1FeatureDirection': shap_direction[0],
            'Top2FeatureDesc': top_features[1],
            'Top2FeatureDirection': shap_direction[1],
            'Top3FeatureDesc': top_features[2],
            'Top3FeatureDirection': shap_direction[2],
            'Top4FeatureDesc': top_features[3],
            'Top4FeatureDirection': shap_direction[3],
            'Top5FeatureDesc': top_features[4],
            'Top5FeatureDirection': shap_direction[4]
        }, index=ix)  # Note: same index as original data for merging
    except:
        return pd.DataFrame()
