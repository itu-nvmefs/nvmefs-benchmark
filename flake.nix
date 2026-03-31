{
  description = "Benchmark project for nvmefs";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell rec {
          packages = with pkgs; [
            autoconf
            automake
            boost
            ccache
            cmake
            gcc
            libaio
            libarchive
            libbsd
            libtool
            liburing
            meson
            nasm
            ninja
            numactl
            openssl
            pkg-config
            python3
            spdk
            util-linux.dev
            yasm
            zlib
            python3
            (python3.withPackages (
              ps: with ps; [
                stdenv.cc.cc.lib
                virtualenv
                pip
                matplotlib
              ]
            ))
          ];
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath packages;
          GEN = "ninja";
          shellHook = ''
            export CPATH="$HOME/xnvme/include:$CPATH"
            export XNVME_LIB="$HOME/xnvme/builddir/lib/libxnvme.so"
            if [ ! -d ".venv" ]; then
              echo "Creating new python virtual environment..."
              python -m venv .venv
            fi
            source .venv/bin/activate
            echo "Python virtual environment activated."
          '';
        };
      }
    );
}
