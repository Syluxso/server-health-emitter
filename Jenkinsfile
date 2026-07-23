pipeline {
    agent any

    environment {
        DEPLOY_DIR = '/opt/services/admin-gateway-sse'
        BINARY_NAME = 'admin-gateway-sse'
        SUPERVISOR_PROGRAM = 'admin-gateway-sse'
    }

    stages {
        stage('Build') {
            steps {
                sh '''
                    set -e
                    go version
                    CGO_ENABLED=0 go build -o "${BINARY_NAME}" .
                    test -x "./${BINARY_NAME}"
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    set -e
                    sudo mkdir -p "${DEPLOY_DIR}"
                    # Cannot overwrite a running ELF in place (ETXTBSY). Write beside it, then atomic replace.
                    sudo cp "./${BINARY_NAME}" "${DEPLOY_DIR}/${BINARY_NAME}.new"
                    sudo chmod 755 "${DEPLOY_DIR}/${BINARY_NAME}.new"
                    sudo mv -f "${DEPLOY_DIR}/${BINARY_NAME}.new" "${DEPLOY_DIR}/${BINARY_NAME}"
                    sudo chown root:root "${DEPLOY_DIR}/${BINARY_NAME}"
                    if [ -f ./start.sh ]; then
                        sudo cp ./start.sh "${DEPLOY_DIR}/start.sh"
                        sudo chmod 755 "${DEPLOY_DIR}/start.sh"
                        sudo chown root:root "${DEPLOY_DIR}/start.sh"
                    fi
                    sudo supervisorctl reread
                    sudo supervisorctl update "${SUPERVISOR_PROGRAM}" || true
                    sudo supervisorctl restart "${SUPERVISOR_PROGRAM}"
                    sleep 3
                    sudo supervisorctl status "${SUPERVISOR_PROGRAM}" || true
                    curl -sf "http://127.0.0.1:8097/healthz" || true
                '''
            }
        }
    }

    post {
        success {
            echo 'admin-gateway-sse (server-health-emitter) deployed'
        }
        failure {
            echo 'Build or deploy failed. Ensure Go is on the Jenkins agent PATH and sudoers allow deploy to /opt/services/admin-gateway-sse + supervisorctl.'
        }
    }
}
