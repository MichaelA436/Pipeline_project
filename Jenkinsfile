pipeline {
    agent any

    environment {
        // Explicit Python 3.9 path
        PYTHON_BIN = '/usr/bin/python3.9'
    }

    stages {

        stage('Checkout SCM') {
            steps {
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: '*/main']],
                    userRemoteConfigs: [[url: 'https://github.com/MichaelA436/Pipeline_project.git']]
                ])
            }
        }

        stage('Setup Python Environment') {
            steps {
                script {
                    // Show Python version
                    sh """
                        echo "Using Python binary: $PYTHON_BIN"
                        $PYTHON_BIN --version
                    """

                    // Create virtual environment and install dependencies
                    sh """
                        $PYTHON_BIN -m venv venv
                        . venv/bin/activate
                        pip install --upgrade pip wheel
                        pip install -r requirements.txt
                        python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
                    """
                }
            }
        }

        stage('Run Pytests') {
            steps {
                echo 'Running unit tests with pytest...'
                sh """
                    . venv/bin/activate
                    pytest --maxfail=1 --disable-warnings -q
                """
            }
        }

        stage('Full Load') {
            steps {
                echo 'Full Load stage skipped (placeholder)'
            }
        }

        stage('Cleaning') {
            steps {
                echo 'Cleaning stage skipped (placeholder)'
            }
        }

        stage('Incremental Load') {
            steps {
                echo 'Incremental Load stage skipped (placeholder)'
            }
        }

        stage('Transformation (Hive)') {
            steps {
                echo 'Transformation (Hive) stage skipped (placeholder)'
            }
        }
    }

    post {
        always {
            echo 'Pipeline finished. Check logs and pytest results.'
        }
        failure {
            echo 'Pipeline failed!'
        }
        success {
            echo 'Pipeline succeeded!'
        }
    }
}
