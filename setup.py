from setuptools import setup, find_packages

# Read requirements.txt for dependencies
def parse_requirements():
    with open('requirements.txt', encoding='utf-8') as f:
        lines = f.read().splitlines()
        # Filter out -e, git+ or anything else that confuses setup()
        return [
            line.split('#', 1)[0].strip() for line in lines
            if line.strip() and not line.lstrip().startswith(('-e', '#')) and 'git+' not in line
        ]

setup(
    name='radio-song-analysis',
    version='0.2',
    author='Rui Pereira',
    author_email='ruiarpereira15@gmail.com',
    description='A Streamlit app for radio song sentiment analysis',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Tyainss/radio_song_analysis',
    packages=find_packages(exclude=["venv*"], include=['data_extract']),  # Automatically finds Python packages in your project
    include_package_data=False,  # Ensures non-Python files are included
    install_requires=parse_requirements(),  # Install dependencies from requirements.txt
    python_requires='>=3.11',  # Required by modern ML and imaging dependencies
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)