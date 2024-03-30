import pandas as pd

def read_file(file_path):
    data = []
    classification = None

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line:  
                if line.startswith("Default") or line.startswith("Configuration"):
                    classification = line
                else:
                    data.append((classification, float(line)))

    return data

def calculate_mean(data):
    df = pd.DataFrame(data, columns=['Classification', 'Value'])
    df_mean = df.groupby('Classification').mean()
    return df_mean

def main():
    file_path = 'test_results.txt' 
    data = read_file(file_path)
    df_mean = calculate_mean(data)
    
    df_mean.to_csv("results.csv")

if __name__ == "__main__":
    main()