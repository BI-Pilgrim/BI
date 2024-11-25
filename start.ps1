function New-DevPath {
    [CmdletBinding()]
    param (
        [System.String] $Path,
        [System.String] $ItemType
    )
    if (! (Test-Path -Path $Path)) {
        Write-Debug -Message ("Creating " + $ItemType + " " + $Path)
        New-Item -Path $Path -ItemType $ItemType
    }
}

New-DevPath -Path ./.dev/ -ItemType Directory
New-DevPath -Path ./.dev/.python_history -ItemType File
New-DevPath -Path ./.dev/.bash_history -ItemType File

docker compose down; docker compose up --build