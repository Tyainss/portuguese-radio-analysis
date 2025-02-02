from setuptools import setup, find_packages

# Read requirements.txt for dependencies
def parse_requirements():
    with open('requirements.txt') as f:
        lines = f.read().splitlines()
        # Filter out -e, git+ or anything else that confuses setup()
        return [
            line for line in lines
            if line and not line.startswith('-e') and not line.startswith('#') and 'git+' not in line
        ]

setup(
    name='radio-song-analysis',
    version='0.2',
    author='Rui Pereira',
    author_email='ruiarpereira15@gmail.com',
    description='A Streamlit app for radio song sentiment analysis',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Tyainss/radio_song_analysis',
    packages=find_packages(exclude=["venv*"], include=['data_extract']),  # Automatically finds Python packages in your project
    include_package_data=False,  # Ensures non-Python files are included
    install_requires=parse_requirements(),  # Install dependencies from requirements.txt
    python_requires='>=3.8',  # Specify Python version compatibility
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)