download:   
    main: |
        # All downloads

        All published releases are available at
        [http://tarantool.org/dist/master].

        # How to choose the right version for download

        Tarantool uses a 3-digit versioning scheme `<major>-<minor>-<patch>`.
        Major digits change rarely. A minor version increase indicates one
        or few incompatibile changes. Patch verison counter is increased
        whenever the source tree receives a few important bugfixes.

        The version string may also contain a git revision id, to ease
        identification of the unqiue commit used to generate the build.

        The current version of the master branch is **@PACKAGE_VERSION@**.

        An automatic build system creates, tests and publishes packages
        for every push into the master branch. All binary packages contain
        symbol information. Additionally, **-debug-**
        packages contain asserts and are compiled without optimization.

        ## Source tarball

        The latest source archive is [tarantool-@PACKAGE_VERSION@-src.tar.gz]
        Please consult with README for build instructions on your system.
        
        [tarantool-@PACKAGE_VERSION@-src.tar.gz]: http://tarantool.org/dist/master/tarantool-@PACKAGE_VERSION@-src.tar.gz 

        ## Binary downloads

        To simplify problem analysis and avoid various bugs induced
        by compilation parameters and environment, it is recommended
        that production systems use the builds provided on this site.

        ### Debian GNU/Linux and Ubuntu

        We maintain an always up-to-date Debian GNU/Linux and Ubuntu package
        repository at [http://tarantool.org/dist/master/debian] and
        [http://tarantool.org/dist/master/ubuntu]
        respectively.

        At the moment the repository contains builds for Debian "Sid", "Jessie",
        "Wheezy" and Ubuntu "Precise", "Quantal", "Raring", "Saucy".
        It can be added to your apt sources list with:
        
        ```bash
        wget http://tarantool.org/dist/public.key
        sudo apt-key add ./public.key
        release=`lsb_release -c -s`

        # For Debian:
        cat > /etc/apt/sources.list.d/tarantool.list <<- EOF
        deb http://tarantool.org/dist/master/debian/ $release main
        deb-src http://tarantool.org/dist/master/debian/ $release main
        EOF

        # For Ubuntu:
        cat > /etc/apt/sources.list.d/tarantool.list <<- EOF
        deb http://tarantool.org/dist/master/ubuntu/ $release main
        deb-src http://tarantool.org/dist/master/ubuntu/ $release main
        EOF

        sudo apt-get update
        sudo apt-get install tarantool
        ```
        
        ### CentOS 5-6 and RHEL 5-6

        CentOS repository is available at [http://tarantool.org/dist/master/centos]

        Add the following section to your yum repository list (/etc/yum.repos.d/tarantool.repo)
        to enable it:
        
        ```ini
        [tarantool]
        name=CentOS-$releasever - Tarantool
        baseurl=http://tarantool.org/dist/master/centos/$releasever/os/$basearch/
        enabled=1
        gpgcheck=0
        ```
        
        ### Fedora

        Fedora repository is available at [http://tarantool.org/dist/master/fedora]

        Add the following section to your yum repository list (/etc/yum.repos.d/tarantool.repo)
        to enable it:

        ```ini
        [tarantool]
        name=Fedora-$releasever - Tarantool
        baseurl=http://tarantool.org/dist/master/fedora/$releasever/$basearch/
        enabled=1
        gpgcheck=0
        ```

        ### Gentoo Linux

        Tarantool is available from `tarantool` portage overlay. Use
        [layman] to add the overlay to your system:

        ```
        # layman -S
        # layman -a tarantool
        # emerge dev-db/tarantool -av
        ```
        
        [layman]: http://wiki.gentoo.org/wiki/Layman

        ### FreeBSD
        
        Tarantool is available from the FreeBSD Ports collection
        (`databases/tarantool`). 

        ### OS X

        You can install Tarantool using homebrew:
        
        ```
        $ brew install https://raw.githubusercontent.com/tarantool/tarantool/stable/extra/tarantool.rb
        ```
        
        Please upgrade `clang` to version 3.2 or later using
        `Command Line Tools for Xcode` disk image version 4.6+ from
        [Apple Developer] web-site.

        [Apple Developer]: https://developer.apple.com/downloads/

        # Old master branch

        In the same manner as for [the master branch][master], every push into
        [the old master][stable] is [available online][builds-s].
        The server bugs database is maintained on [Github][issues].
        
        [stable]:   http://github.com/tarantool/tarantool/tree/stable
        [master]:   http://github.com/tarantool/tarantool/tree/master
        [builds-s]: http://tarantool.org/dist/stable
        [issues]:   http://github.com/tarantool/tarantool/issues

        ## Connectors

        - Perl driver, [DR:Tarantool](http://search.cpan.org/~unera/DR-Tarantool-0.42/lib/DR/Tarantool.pm)
        - Java driver, [Maven repository](http://dgreenru.github.com/tarantool-java)
        - Ruby driver, [https://github.com/mailru/tarantool-ruby]
        - Python driver, [http://pypi.python.org/pypi/tarantool]
        - PHP driver, [https://github.com/tarantool/tarantool-php]
        - node.js driver, [https://github.com/devgru/node-tarantool]
        - Erlang driver, [https://github.com/rtsisyk/etarantool]
        - C connector [https://github.com/tarantool/tarantool-c]
        
        [http://tarantool.org/dist/master]: http://tarantool.org/dist/master
        [http://tarantool.org/dist/master/debian]: http://tarantool.org/dist/master/debian
        [http://tarantool.org/dist/master/ubuntu]: http://tarantool.org/dist/master/ubuntu
        [http://tarantool.org/dist/master/centos]: http://tarantool.org/dist/master/centos
        [http://tarantool.org/dist/master/fedora]: http://tarantool.org/dist/master/fedora
        [https://github.com/mailru/tarantool-ruby]: https://github.com/mailru/tarantool-ruby
        [http://pypi.python.org/pypi/tarantool]: http://pypi.python.org/pypi/tarantool
        [https://github.com/tarantool/tarantool-php]: https://github.com/tarantool/tarantool-php
        [https://github.com/devgru/node-tarantool]: https://github.com/devgru/node-tarantool
        [https://github.com/rtsisyk/etarantool]: https://github.com/rtsisyk/etarantool
