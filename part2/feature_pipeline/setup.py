import setuptools

setuptools.setup(
    name='feature_pipeline',
    version='1.0.0',
    install_requires=["apache-beam[gcp]>=2.43.0"],
    packages=setuptools.find_packages(),
)