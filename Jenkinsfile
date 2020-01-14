pipeline {
    agent {
        docker {
            image 'project-tsurugi/oltp-sandbox'
            label 'docker'
            args '--privileged --ulimit memlock=-1:-1 --ulimit nofile=655360:655360 --ulimit nproc=655360:655360 --ulimit rtprio=99:99 --sysctl kernel.shmmax=9223372036854775807 --sysctl kernel.shmall=1152921504606846720 --sysctl kernel.shmmni=409600'
        }
    }
    environment {
        GITHUB_URL = 'https://github.com/project-tsurugi/sharksfin'
        GITHUB_CHECKS = 'tsurugi-check'
        BUILD_PARALLEL_NUM="""${sh(
                returnStdout: true,
                script: 'grep processor /proc/cpuinfo | wc -l'
            )}"""
    }
    stages {
        stage ('Prepare env') {
            steps {
                sh '''
                    ssh-keyscan -t rsa github.com > ~/.ssh/known_hosts
                '''
            }
        }
        stage ('checkout master') {
            steps {
                checkout scm
                sh '''
                    git clean -dfx
                    git submodule sync --recursive
                    git submodule update --init --recursive
                '''
            }
        }
        stage ('Install foedus') {
            steps {
                sh '''
                    cd third_party/foedus
                    git log -n 1 --format=%H
                    # git clean -dfx
                    mkdir -p build
                    cd build
                    rm -f CMakeCache.txt
                    cmake -DCMAKE_BUILD_TYPE=Debug -DGFLAGS_INTTYPES_FORMAT=C99 -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/.local ..
                    make all install -j${BUILD_PARALLEL_NUM}
                '''
            }
        }
        stage ('Install kvs_charkey') {
            steps {
                sh '''
                    cd third_party/kvs_charkey
                    ./bootstrap.sh
                    mkdir build
                    cd build
                    cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON -DENABLE_SANITIZER=ON -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/.local ..
                    make clean
                    make all install -j${BUILD_PARALLEL_NUM}
                '''
            }
        }

        stage ('Build') {
            steps {
                sh '''
                    mkdir build
                    cd build
                    cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_FOEDUS_BRIDGE=ON -DENABLE_COVERAGE=ON -DCMAKE_PREFIX_PATH=${WORKSPACE}/.local -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
                    make all -j${BUILD_PARALLEL_NUM}
                '''
            }
        }
        stage ('Test') {
            environment {
                GTEST_OUTPUT="xml"
                ASAN_OPTIONS="detect_stack_use_after_return=true"
            }
            steps {
                sh '''
                    cd build
                    make test ARGS="--verbose"
                '''
            }
        }
        stage ('Doc') {
            steps {
                sh '''
                    cd build
                    make doxygen > doxygen.log 2>&1
                    zip -q -r sharksfin-doxygen doxygen/html
                '''
            }
        }
        stage ('Coverage') {
            environment {
                GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e .*/antlr.*'
                BUILD_PARALLEL_NUM=4
            }
            steps {
                sh '''
                    cd build
                    mkdir gcovr-xml gcovr-html
                    gcovr -j ${BUILD_PARALLEL_NUM} -r .. --xml ${GCOVR_COMMON_OPTION} -o gcovr-xml/sharksfin-gcovr.xml
                    gcovr -j ${BUILD_PARALLEL_NUM} -r .. --html --html-details --html-title "sharksfin coverage" ${GCOVR_COMMON_OPTION} -o gcovr-html/sharksfin-gcovr.html
                    zip -q -r sharksfin-coverage-report gcovr-html
                '''
            }
        }
        stage ('Lint') {
            steps {
                sh '''#!/bin/bash
                    python tools/bin/run-clang-tidy.py -quiet -export-fixes=build/clang-tidy-fix.yaml -p build -extra-arg=-Wno-unknown-warning-option -header-filter=$(pwd)'/(include|memory|mock|foedus|examples)/.*\\.h$' $(pwd)'/(memory/src|mock/src|foedus/src|examples)/.*' > build/clang-tidy.log 2> build/clang-tidy-error.log
                '''
            }
        }
        stage ('Graph') {
            steps {
                sh '''
                    cd build
                    cp ../cmake/CMakeGraphVizOptions.cmake .
                    cmake --graphviz=dependency-graph/sharksfin.dot ..
                    cd dependency-graph
                    dot -T png sharksfin.dot -o sharksfin.png
                '''
            }
        }
    }
    post {
        always {
            xunit tools: ([GoogleTest(pattern: '**/*_gtest_result.xml', deleteOutputFiles: false, failIfNotNew: false, skipNoTestFiles: true, stopProcessingIfError: true)]), reduceLog: false
            recordIssues tool: clangTidy(pattern: 'build/clang-tidy.log'),
                qualityGates: [[threshold: 1, type: 'TOTAL', unstable: true]]
            recordIssues tool: gcc4(),
                enabledForFailure: true
            recordIssues tool: doxygen(pattern: 'build/doxygen.log')
            recordIssues tool: taskScanner(
                highTags: 'FIXME', normalTags: 'TODO',
                includePattern: '**/*.md,**/*.txt,**/*.in,**/*.cmake,**/*.cpp,**/*.h',
                excludePattern: 'third_party/**'),
                enabledForFailure: true
            publishCoverage adapters: [coberturaAdapter('build/gcovr-xml/sharksfin-gcovr.xml')], sourceFileResolver: sourceFiles('STORE_ALL_BUILD')
            archiveArtifacts allowEmptyArchive: true, artifacts: 'build/sharksfin-coverage-report.zip, build/sharksfin-doxygen.zip, build/clang-tidy.log, build/clang-tidy-fix.yaml, build/dependency-graph/sharksfin.png', onlyIfSuccessful: true
            notifySlack('tsurugi-dev')
        }
    }
}
