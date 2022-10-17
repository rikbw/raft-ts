
import split2 from 'split2'

process.stdin.pipe(split2()).on('data', handleInput)

function handleInput(input: string) {
    console.log('you sent: ', input)
}
